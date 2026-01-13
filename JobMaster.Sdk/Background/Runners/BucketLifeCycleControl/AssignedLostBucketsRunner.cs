using JobMaster.Contracts.Extensions;
using JobMaster.Contracts.Models;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.BucketLifeCycleControl;

public class AssignedLostBucketsRunner : JobMasterRunner
{
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly IMasterAgentWorkersService masterAgentWorkersService;
    private readonly JobMasterLockKeys lockKeys;
    
    public override TimeSpan SucceedInterval { get; }
        
    public AssignedLostBucketsRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker, 
        TimeSpan? succeedInterval = null) : 
        base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        lockKeys = new JobMasterLockKeys(this.BackgroundAgentWorker.ClusterConnConfig.ClusterId);
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        masterAgentWorkersService = backgroundAgentWorker.GetClusterAwareService<IMasterAgentWorkersService>();
        
        SucceedInterval = succeedInterval ?? TimeSpan.FromMinutes(1);
    }
    
    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (BackgroundAgentWorker.StopRequested)
        {
            return OnTickResult.Skipped(this);
        }
        
        var lockToken = masterDistributedLockerService.TryLock(lockKeys.BucketRunnerLock(), TimeSpan.FromMinutes(5));
        if (lockToken == null)
        {
            return OnTickResult.Locked(this);
        }
        
        var buckets = await masterBucketsService.QueryAllNoCacheAsync(BucketStatus.Lost);
        var lostBuckets = buckets
            .Where(b => b.Status == BucketStatus.Lost);
        
        var workers = await masterAgentWorkersService.GetWorkersAsync(useCache: false);
        var workersAlive = workers.Where(x => x.IsAlive).ToList();
        
        foreach (var bucket in lostBuckets)
        {
            ct.ThrowIfCancellationRequested();
            
            var bucketLockToken = this.masterDistributedLockerService.TryLock(lockKeys.BucketLock(bucket.Id), TimeSpan.FromSeconds(10));
            if (bucketLockToken == null)
            {
                continue;
            }
            
            var workerToSelect = workersAlive.Where(x => x.AgentConnectionId.IdValue == bucket.AgentConnectionId.IdValue && x.Mode != AgentWorkerMode.Coordinator).ToList();
            if (!workerToSelect.Any())
            {
                logger.Critical($"Worker not found for bucket {bucket.Id}", JobMasterLogSubjectType.Bucket, bucket.Id);
                this.masterDistributedLockerService.ReleaseLock(lockKeys.BucketLock(bucket.Id), bucketLockToken);
                continue;
            }
                
            if (workerToSelect.Any(x => x.Mode == AgentWorkerMode.Drain) && JobMasterRandomUtil.GetBoolean(0.75))
            {
                workerToSelect = workerToSelect.Where(x => x.Mode == AgentWorkerMode.Drain).ToList();
            }
            
            var worker = workerToSelect.Random()!;

            if (bucket.ReadyToDrain(worker.Id))
            {
                logger.Info($"Bucket {bucket.Id} is ready to drain", JobMasterLogSubjectType.Bucket, bucket.Id);
                await masterBucketsService.UpdateAsync(bucket);
            }
            
            this.masterDistributedLockerService.ReleaseLock(lockKeys.BucketLock(bucket.Id), bucketLockToken);
        }
        
        
        this.masterDistributedLockerService.ReleaseLock(lockKeys.BucketRunnerLock(), lockToken);
        
        return OnTickResult.Success(this);
    }

    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(10);
}