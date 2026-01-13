using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.CleanUpData;

/// <summary>
/// Periodically deletes recurring schedules in Inactive or Canceled status older than DataRetentionTtl,
/// based on CreatedAt. Not bucket-aware and guarded by a cluster-wide distributed lock.
/// </summary>
public sealed class DeleteOldInactiveRecurringSchedulesRunner : JobMasterRunner
{
    private readonly IMasterClusterConfigurationService clusterConfigService;
    private readonly IMasterRecurringSchedulesRepository schedulesRepo;
    private readonly IMasterDistributedLockerService locker;
    private readonly JobMasterLockKeys lockKeys;

    public DeleteOldInactiveRecurringSchedulesRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        clusterConfigService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
        schedulesRepo = backgroundAgentWorker.GetClusterAwareRepository<IMasterRecurringSchedulesRepository>();
        locker = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
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
            var deleted = await schedulesRepo.PurgeTerminatedAsync(cutoff, BackgroundAgentWorker.BatchSize);
            var next = deleted >= BackgroundAgentWorker.BatchSize ? burstNext : desiredNext;
            return OnTickResult.Success(next);
        }
        finally
        {
            locker.ReleaseLock(lockKeys.RecurringSchedulesCleanupLock(), lockToken);
        }
    }

    public override TimeSpan SucceedInterval => TimeSpan.FromHours(1);
}
