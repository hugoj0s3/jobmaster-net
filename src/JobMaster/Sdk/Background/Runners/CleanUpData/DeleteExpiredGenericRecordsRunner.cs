using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.CleanUpData;

/// <summary>
/// Deletes generic records whose per-record expiry has passed (<c>UtcNow</c> is used as
/// the cutoff rather than a cluster-level TTL). Expired records are short-lived entries
/// that don't need to persist beyond a fixed window — heartbeats, sentinel change events,
/// etc. A distributed lock prevents concurrent deletes across coordinator workers.
/// A <see cref="ConsecutiveBurstLimiter"/> shortens the next interval when a full batch
/// was deleted, allowing the runner to drain large backlogs quickly before returning to
/// its normal <see cref="SucceedInterval"/>.
/// </summary>
internal sealed class DeleteExpiredGenericRecordsRunner : JobMasterRunner
{
    private readonly IMasterGenericRecordRepository masterGenericRecordRepository;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly JobMasterLockKeys lockKeys;
    
    private readonly ConsecutiveBurstLimiter burstLimiter;

    public DeleteExpiredGenericRecordsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        masterGenericRecordRepository = backgroundAgentWorker.GetClusterAwareRepository<IMasterGenericRecordRepository>();
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
        burstLimiter = new ConsecutiveBurstLimiter(10, BackgroundAgentWorker.TransferBatchSize);
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (BackgroundAgentWorker.StopRequested)
        {
            return OnTickResult.Skipped(this);
        }

        var desiredNext = SucceedInterval;
        var burstNext = TimeSpan.FromMinutes(5);

        // Acquire cluster-wide lock to avoid parallel execution
        var lockDuration = burstNext + TimeSpan.FromMinutes(1);
        var lockToken = masterDistributedLockerService.TryLock(lockKeys.GenericRecordsCleanupLock(), lockDuration);
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromMinutes(2));
        }

        try
        {
            // Delete up to BatchSize expired records each tick
            var deleted = await masterGenericRecordRepository.DeleteExpiredAsync(DateTime.UtcNow, BackgroundAgentWorker.TransferBatchSize);
            var next = burstLimiter.Next(desiredNext, burstNext, deleted);
            return OnTickResult.Success(next);
        }
        finally
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.GenericRecordsCleanupLock(), lockToken);
        }
    }

    public override TimeSpan SucceedInterval => TimeSpan.FromHours(1);
}
