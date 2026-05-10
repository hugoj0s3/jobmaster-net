using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.CleanUpData;

/// <summary>
/// Purges terminated recurring schedules from the master repository once they are older
/// than the cluster's <c>DataRetentionTtl</c> (only schedules with a non-null
/// <c>TerminatedAt</c> are eligible).
/// Skipped when no TTL is configured. A distributed lock prevents concurrent purges across
/// coordinator workers. A <see cref="ConsecutiveBurstLimiter"/> shortens the next interval
/// when a full batch was deleted, allowing the runner to drain large backlogs quickly before
/// returning to its normal <see cref="SucceedInterval"/>
/// </summary>
internal sealed class DeleteOldInactiveRecurringSchedulesRunner : JobMasterRunner
{
    private readonly IMasterClusterConfigurationService clusterConfigService;
    private readonly IMasterRecurringSchedulesRepository schedulesRepo;
    private readonly IMasterDistributedLockerService locker;
    private readonly JobMasterLockKeys lockKeys;
    private readonly ConsecutiveBurstLimiter burstLimiter;

    public DeleteOldInactiveRecurringSchedulesRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        clusterConfigService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
        schedulesRepo = backgroundAgentWorker.GetClusterAwareRepository<IMasterRecurringSchedulesRepository>();
        locker = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
        burstLimiter = new ConsecutiveBurstLimiter(10, BackgroundAgentWorker.TransferBatchSize);
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (BackgroundAgentWorker.StopRequested)
            return OnTickResult.Skipped(this);

        var cfg = clusterConfigService.Get();
        var ttl = cfg?.DataRetentionTtl;
        if (ttl == null)
        {
            return OnTickResult.Skipped(SucceedInterval);
        }

        var cutoff = DateTime.UtcNow - ttl.Value;

        var desiredNext = SucceedInterval;
        var burstNext = TimeSpan.FromMinutes(5);
        var lockDuration = burstNext + TimeSpan.FromMinutes(1);

        var lockToken = locker.TryLock(lockKeys.RecurringSchedulesCleanupLock(), lockDuration);
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromMinutes(2));
        }

        try
        {
            var deleted = await schedulesRepo.PurgeTerminatedAsync(cutoff, BackgroundAgentWorker.TransferBatchSize);
            var next = burstLimiter.Next(desiredNext, burstNext, deleted);
            return OnTickResult.Success(next);
        }
        finally
        {
            locker.ReleaseLock(lockKeys.RecurringSchedulesCleanupLock(), lockToken);
        }
    }

    public override TimeSpan SucceedInterval => TimeSpan.FromHours(1);
}
