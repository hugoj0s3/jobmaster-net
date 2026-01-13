using JobMaster.Sdk.Background.Runners.DrainRunners;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Background.Runners;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.BucketLifeCycleControl;

public class DrainRunnersCoordinator : JobMasterRunner
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
        var lockToken = masterDistributedLockerService.TryLock(lockKeys.BucketRunnerLock(), TimeSpan.FromMinutes(5));
        if (lockToken == null)
        {
            return OnTickResult.Skipped(this);
        }
        
        await CleanupDrainRunnersAsync(ct);
        await CreateDrainRunners(ct);

        this.masterDistributedLockerService.ReleaseLock(lockKeys.BucketRunnerLock(), lockToken);
        
        return OnTickResult.Success(this);
    }

    private async Task CleanupDrainRunnersAsync(CancellationToken ct)
    {
        var drainRunners = BackgroundAgentWorker.Runners
            .OfType<IDrainJobsRunner>()
            .Cast<IBucketAwareRunner>()
            .ToList();

        var readyToDeleteRunners = BackgroundAgentWorker.Runners
            .OfType<DrainBucketReadyToDeleteRunner>()
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
        var buckets = await masterBucketsService.QueryAllNoCacheAsync(BucketStatus.ReadyToDrain);
        
        var bucketToDrain = buckets
            .Where(b => b.Status == BucketStatus.ReadyToDrain)
            .Where(b => b.AgentWorkerId == BackgroundAgentWorker.AgentWorkerId)
            .ToList();
        
        var savePendingRunners = BackgroundAgentWorker.Runners.OfType<IDrainSavePendingJobsRunner>().ToList();
        var processingRunners = BackgroundAgentWorker.Runners.OfType<IDrainProcessingJobsRunner>().ToList();
        var recurringScheduleRunners = BackgroundAgentWorker.Runners.OfType<IDrainSavePendingRecurringScheduleRunner>().ToList();
        var readyToDeleteRunners = BackgroundAgentWorker.Runners.OfType<DrainBucketReadyToDeleteRunner>().ToList();
   
        foreach (var bucket in bucketToDrain)
        {
            ct.ThrowIfCancellationRequested();
            
            var bucketLockToken = masterDistributedLockerService.TryLock(lockKeys.BucketLock(bucket.Id), TimeSpan.FromMinutes(5));
            if (bucketLockToken == null)
            {
                continue;
            }

            if (!bucket.MarkAsDraining(BackgroundAgentWorker.AgentWorkerId))
            {
                this.masterDistributedLockerService.ReleaseLock(lockKeys.BucketLock(bucket.Id), bucketLockToken);
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
                var readyToDeleteRunner = new DrainBucketReadyToDeleteRunner(BackgroundAgentWorker);
                readyToDeleteRunner.DefineBucketId(bucket.Id);
                BackgroundAgentWorker.Runners.Add(readyToDeleteRunner);
                await readyToDeleteRunner.StartAsync();
            }
            
            await masterBucketsService.UpdateAsync(bucket);
            logger.Info($"Bucket {bucket.Id} is draining", JobMasterLogSubjectType.Bucket, bucket.Id);
            
            this.masterDistributedLockerService.ReleaseLock(lockKeys.BucketLock(bucket.Id), bucketLockToken);
        }
    }

    public override TimeSpan SucceedInterval { get; }
    
    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(10);
}