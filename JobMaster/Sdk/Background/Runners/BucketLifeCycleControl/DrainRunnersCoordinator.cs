using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Runners.DrainRunners;

namespace JobMaster.Sdk.Background.Runners.BucketLifeCycleControl;

/// <summary>
/// Coordinates the lifecycle of drain runners for this worker.
/// On each tick, stops drain and ready-to-delete runners whose bucket is no longer
/// in a <c>Draining</c> or <c>ReadyToDrain</c> state, then creates new runner sets
/// (save-pending jobs, processing jobs, save-pending recurring schedules, ready-to-delete)
/// for any <c>ReadyToDrain</c> bucket owned by the current worker. Transitions the bucket
/// to <c>Draining</c> and starts all runners atomically under a per-bucket distributed lock.
/// Runs every <see cref="SucceedInterval"/>.
/// </summary>
internal class DrainRunnersCoordinator : JobMasterRunner
{
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private JobMasterLockKeys lockKeys;

    public DrainRunnersCoordinator(IJobMasterBackgroundAgentWorker backgroundAgentWorker, TimeSpan? succeedInterval = null) : 
        base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        SucceedInterval = succeedInterval ?? TimeSpan.FromMinutes(1);
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        lockKeys = new JobMasterLockKeys(this.BackgroundAgentWorker.ClusterConnConfig.ClusterId);
    }
    
    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (BackgroundAgentWorker.StopRequested)
        {
            return OnTickResult.Skipped(this);
        }

        var lockToken = masterDistributedLockerService.TryLock(lockKeys.BucketRunnerLock(), TimeSpan.FromMinutes(2.5));
        if (lockToken == null)
        {
            return OnTickResult.Skipped(this);
        }

        try
        {
            await CleanupDrainRunnersAsync(ct);
            await CreateDrainRunners(ct);
            await RecoverDrainRunnersAsync(ct);
        }
        finally
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.BucketRunnerLock(), lockToken);
        }

        return OnTickResult.Success(this);
    }

    private async Task CleanupDrainRunnersAsync(CancellationToken ct)
    {
        var drainRunners = BackgroundAgentWorker.Runners
            .OfType<IDrainJobsRunner>()
            .Cast<IBucketAwareRunner>()
            .ToList();

        var readyToDeleteRunners = BackgroundAgentWorker.Runners
            .OfType<MarkBucketReadyToDeleteRunner>()
            .Cast<IBucketAwareRunner>()
            .ToList();

        var runnersToEvaluate = drainRunners
            .Concat(readyToDeleteRunners)
            .Where(r => !string.IsNullOrEmpty(r.BucketId))
            .ToList();

        foreach (var runner in runnersToEvaluate)
        {
            ct.ThrowIfCancellationRequested();

            var bucketId = runner.BucketId!;
            var bucket = masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
            if (bucket == null || (bucket.Status != BucketStatus.Draining && bucket.Status != BucketStatus.ReadyToDrain))
            {
                await runner.StopAsync();
            }
        }
    }

    private async Task CreateDrainRunners(CancellationToken ct)
    {
        var readyToDrainBuckets = await masterBucketsService.QueryAllNoCacheAsync(BucketStatus.ReadyToDrain);

        var bucketToDrain = readyToDrainBuckets
            .Where(b => b.Status == BucketStatus.ReadyToDrain)
            .Where(b => b.AgentWorkerId == BackgroundAgentWorker.AgentWorkerId)
            .ToList();

        var savePendingRunners = BackgroundAgentWorker.Runners.OfType<IDrainSavePendingJobsRunner>().ToList();
        var processingRunners = BackgroundAgentWorker.Runners.OfType<IDrainProcessingJobsRunner>().ToList();
        var recurringScheduleRunners = BackgroundAgentWorker.Runners.OfType<IDrainSavePendingRecurringScheduleRunner>().ToList();
        var readyToDeleteRunners = BackgroundAgentWorker.Runners.OfType<MarkBucketReadyToDeleteRunner>().ToList();

        foreach (var bucket in bucketToDrain)
        {
            ct.ThrowIfCancellationRequested();

            if (BackgroundAgentWorker.StopRequested)
            {
                break;
            }

            var bucketLockToken = masterDistributedLockerService.TryLock(lockKeys.BucketLock(bucket.Id), TimeSpan.FromSeconds(10));
            if (bucketLockToken == null)
            {
                continue;
            }

            try
            {
                if (!bucket.MarkAsDraining(BackgroundAgentWorker.AgentWorkerId))
                {
                    continue;
                }

                if (!savePendingRunners.Any(r => r.BucketId == bucket.Id))
                {
                    var savePendingRunner = BackgroundAgentWorker.BucketRunnersFactory
                        .NewDrainSavePendingJobsRunner(BackgroundAgentWorker, BackgroundAgentWorker.AgentConnectionId);
                    savePendingRunner.DefineBucketId(bucket.Id);
                    await savePendingRunner.StartAsync();
                }

                if (!processingRunners.Any(r => r.BucketId == bucket.Id))
                {
                    var processingRunner = BackgroundAgentWorker.BucketRunnersFactory
                        .NewDrainProcessingJobsRunner(BackgroundAgentWorker, BackgroundAgentWorker.AgentConnectionId);
                    processingRunner.DefineBucketId(bucket.Id);
                    await processingRunner.StartAsync();
                }

                if (!recurringScheduleRunners.Any(r => r.BucketId == bucket.Id))
                {
                    var saveRecurringScheduleRunner = BackgroundAgentWorker.BucketRunnersFactory
                        .NewDrainSavePendingRecurringScheduleRunner(BackgroundAgentWorker, BackgroundAgentWorker.AgentConnectionId);
                    saveRecurringScheduleRunner.DefineBucketId(bucket.Id);
                    await saveRecurringScheduleRunner.StartAsync();
                }

                if (!readyToDeleteRunners.Any(r => r.BucketId == bucket.Id))
                {
                    var readyToDeleteRunner = new MarkBucketReadyToDeleteRunner(BackgroundAgentWorker);
                    readyToDeleteRunner.DefineBucketId(bucket.Id);
                    await readyToDeleteRunner.StartAsync();
                }

                await masterBucketsService.UpdateAsync(bucket);
                logger.Info($"Bucket {bucket.Id} is draining", JobMasterLogCategory.Bucket, bucket.Id);
            }
            finally
            {
                masterDistributedLockerService.ReleaseLock(lockKeys.BucketLock(bucket.Id), bucketLockToken);
            }
        }
    }

    private async Task RecoverDrainRunnersAsync(CancellationToken ct)
    {
        var drainingBuckets = await masterBucketsService.QueryAllNoCacheAsync(BucketStatus.Draining);

        var bucketsToRecover = drainingBuckets
            .Where(b => b.Status == BucketStatus.Draining)
            .Where(b => b.AgentWorkerId == BackgroundAgentWorker.AgentWorkerId)
            .ToList();

        var savePendingRunners = BackgroundAgentWorker.Runners.OfType<IDrainSavePendingJobsRunner>().ToList();
        var processingRunners = BackgroundAgentWorker.Runners.OfType<IDrainProcessingJobsRunner>().ToList();
        var recurringScheduleRunners = BackgroundAgentWorker.Runners.OfType<IDrainSavePendingRecurringScheduleRunner>().ToList();
        var readyToDeleteRunners = BackgroundAgentWorker.Runners.OfType<MarkBucketReadyToDeleteRunner>().ToList();

        foreach (var bucket in bucketsToRecover)
        {
            ct.ThrowIfCancellationRequested();

            if (BackgroundAgentWorker.StopRequested)
            {
                break;
            }

            var missing = new List<string>();

            if (!savePendingRunners.Any(r => r.BucketId == bucket.Id))
                missing.Add(nameof(IDrainSavePendingJobsRunner));

            if (!processingRunners.Any(r => r.BucketId == bucket.Id))
                missing.Add(nameof(IDrainProcessingJobsRunner));

            if (!recurringScheduleRunners.Any(r => r.BucketId == bucket.Id))
                missing.Add(nameof(IDrainSavePendingRecurringScheduleRunner));

            if (!readyToDeleteRunners.Any(r => r.BucketId == bucket.Id))
                missing.Add(nameof(MarkBucketReadyToDeleteRunner));

            if (!missing.Any())
            {
                continue;
            }

            logger.Warn($"Draining bucket {bucket.Id} has missing runners: {string.Join(", ", missing)}. Recovering.", JobMasterLogCategory.Bucket, bucket.Id);

            if (!savePendingRunners.Any(r => r.BucketId == bucket.Id))
            {
                var savePendingRunner = BackgroundAgentWorker.BucketRunnersFactory
                    .NewDrainSavePendingJobsRunner(BackgroundAgentWorker, BackgroundAgentWorker.AgentConnectionId);
                savePendingRunner.DefineBucketId(bucket.Id);
                await savePendingRunner.StartAsync();
            }

            if (!processingRunners.Any(r => r.BucketId == bucket.Id))
            {
                var processingRunner = BackgroundAgentWorker.BucketRunnersFactory
                    .NewDrainProcessingJobsRunner(BackgroundAgentWorker, BackgroundAgentWorker.AgentConnectionId);
                processingRunner.DefineBucketId(bucket.Id);
                await processingRunner.StartAsync();
            }

            if (!recurringScheduleRunners.Any(r => r.BucketId == bucket.Id))
            {
                var saveRecurringScheduleRunner = BackgroundAgentWorker.BucketRunnersFactory
                    .NewDrainSavePendingRecurringScheduleRunner(BackgroundAgentWorker, BackgroundAgentWorker.AgentConnectionId);
                saveRecurringScheduleRunner.DefineBucketId(bucket.Id);
                await saveRecurringScheduleRunner.StartAsync();
            }

            if (!readyToDeleteRunners.Any(r => r.BucketId == bucket.Id))
            {
                var readyToDeleteRunner = new MarkBucketReadyToDeleteRunner(BackgroundAgentWorker);
                readyToDeleteRunner.DefineBucketId(bucket.Id);
                await readyToDeleteRunner.StartAsync();
            }
        }
    }

    public override TimeSpan SucceedInterval { get; }
    
    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(10);
}