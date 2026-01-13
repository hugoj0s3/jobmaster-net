using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.CleanUpData;

/// <summary>
/// Periodically deletes expired GenericRecord entries for the current cluster.
/// Not bucket-aware and uses the base semaphore to serialize execution.
/// </summary>
public sealed class DeleteExpiredGenericRecordsRunner : JobMasterRunner
{
    private readonly IMasterGenericRecordRepository masterGenericRecordRepository;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly JobMasterLockKeys lockKeys;

    public DeleteExpiredGenericRecordsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        masterGenericRecordRepository = backgroundAgentWorker.GetClusterAwareRepository<IMasterGenericRecordRepository>();
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
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
            var deleted = await masterGenericRecordRepository.DeleteExpiredAsync(DateTime.UtcNow, BackgroundAgentWorker.BatchSize);
            var next = deleted >= BackgroundAgentWorker.BatchSize ? burstNext : desiredNext;
            return OnTickResult.Success(next);
        }
        finally
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.GenericRecordsCleanupLock(), lockToken);
        }
    }

    public override TimeSpan SucceedInterval => TimeSpan.FromHours(1);
}
