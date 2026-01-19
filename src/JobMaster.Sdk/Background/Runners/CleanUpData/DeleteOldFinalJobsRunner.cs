using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.CleanUpData;

/// <summary>
/// Periodically deletes finalized jobs older than DataRetentionTtl based on ScheduledAt.
/// Not bucket-aware and uses a distributed lock to avoid parallel runs across workers.
/// </summary>
public sealed class DeleteOldFinalJobsRunner : JobMasterRunner
{
    private readonly IMasterClusterConfigurationService clusterConfigService;
    private readonly IMasterJobsRepository jobsRepo;
    private readonly IMasterDistributedLockerService locker;
    private readonly JobMasterLockKeys lockKeys;
    private readonly ConsecutiveBurstLimiter burstLimiter;

    public DeleteOldFinalJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        clusterConfigService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
        jobsRepo = backgroundAgentWorker.GetClusterAwareRepository<IMasterJobsRepository>();
        locker = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
        burstLimiter = new ConsecutiveBurstLimiter(10, BackgroundAgentWorker.BatchSize);
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

        var lockToken = locker.TryLock(lockKeys.JobsCleanupLock(), lockDuration);
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromMinutes(2));
        }

        try
        {
            var deleted = await jobsRepo.PurgeFinalByScheduledAtAsync(cutoff, BackgroundAgentWorker.BatchSize);
            var next = burstLimiter.Next(desiredNext, burstNext, deleted);
            return OnTickResult.Success(next);
        }
        finally
        {
            locker.ReleaseLock(lockKeys.JobsCleanupLock(), lockToken);
        }
    }

    public override TimeSpan SucceedInterval => TimeSpan.FromHours(1);
}
