using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.BucketLifeCycleControl;

public class MarkBucketAsLostRunner : JobMasterRunner
{
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly IMasterAgentWorkersService masterAgentWorkersService;
    private JobMasterLockKeys lockKeys;
    
    public override TimeSpan SucceedInterval {get;}
        
    public MarkBucketAsLostRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker, TimeSpan? succeedInterval = null) : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        lockKeys = new JobMasterLockKeys(this.BackgroundAgentWorker.ClusterConnConfig.ClusterId);
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        masterAgentWorkersService = backgroundAgentWorker.GetClusterAwareService<IMasterAgentWorkersService>();
        
        SucceedInterval = succeedInterval ?? TimeSpan.FromMinutes(2.5);
    }
    
    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        var lockToken = masterDistributedLockerService.TryLock(lockKeys.BucketRunnerLock(), TimeSpan.FromMinutes(5));
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromSeconds(10));
        }
        
        var buckets = await masterBucketsService.QueryAllNoCacheAsync();
        var bucketsToEvaluate = buckets.Where(
            b => b.AgentWorkerId != BackgroundAgentWorker.AgentWorkerId
            && b.Status != BucketStatus.Lost);
        
        var workers = await masterAgentWorkersService.GetWorkersAsync(useCache: false);
        var bucketsToMarkAsLost = new List<BucketModel>();
        foreach (var bucket in bucketsToEvaluate)
        {
            ct.ThrowIfCancellationRequested();
            
            if (bucket.AgentWorkerId == null)
            {
                bucketsToMarkAsLost.Add(bucket);
                continue;
            }
            
            var agentWorker = workers.FirstOrDefault(x => x.Id == bucket.AgentWorkerId);
            if (agentWorker == null) 
            {
                bucketsToMarkAsLost.Add(bucket);
                continue;
            }
            
            if (!agentWorker.IsAlive) 
            {
                bucketsToMarkAsLost.Add(bucket);
            }
        }

        foreach (var bucket in bucketsToMarkAsLost)
        {
            var bucketLockToken = masterDistributedLockerService.TryLock(lockKeys.BucketLock(bucket.Id), TimeSpan.FromMinutes(5));
            if (bucketLockToken == null)
            {
                continue;
            }
            
            await BackgroundAgentWorker.WorkerClusterOperations.MarkBucketAsLostAsync(bucket.Id);
            masterDistributedLockerService.ReleaseLock(lockKeys.BucketLock(bucket.Id), bucketLockToken);
        }
        
        this.masterDistributedLockerService.ReleaseLock(lockKeys.BucketRunnerLock(), lockToken);
        
        return OnTickResult.Success(this);
    }

    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(30);
}