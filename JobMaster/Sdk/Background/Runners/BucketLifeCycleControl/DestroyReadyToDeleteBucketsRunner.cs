using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.BucketLifeCycleControl;

internal class DestroyReadyToDeleteBucketsRunner : JobMasterRunner
{
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    private readonly JobMasterLockKeys lockKeys;

    public override TimeSpan SucceedInterval => JobMasterConstants.BucketNoJobsBeforeReadyToDelete;

    public DestroyReadyToDeleteBucketsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        lockKeys = new JobMasterLockKeys(this.BackgroundAgentWorker.ClusterConnConfig.ClusterId);
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        var lockToken = masterDistributedLockerService.TryLock(lockKeys.BucketRunnerLock(), TimeSpan.FromMinutes(5));
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromSeconds(10));
        }

        try
        {
            var buckets = await masterBucketsService.QueryAllNoCacheAsync(BucketStatus.ReadyToDelete);
            var nowUtc = DateTime.UtcNow;

            foreach (var bucket in buckets)
            {
                if (bucket.DeletesAt is null || bucket.DeletesAt.Value > nowUtc)
                {
                    continue;
                }

                var bucketLockToken = masterDistributedLockerService.TryLock(lockKeys.BucketLock(bucket.Id), TimeSpan.FromMinutes(5));
                if (bucketLockToken == null)
                {
                    continue;
                }

                try
                {
                    var freshBucket = masterBucketsService.Get(bucket.Id, JobMasterConstants.BucketFastAllowDiscrepancy);
                    if (freshBucket is null || freshBucket.Status != BucketStatus.ReadyToDelete)
                    {
                        continue;
                    }

                    if (freshBucket.DeletesAt is null)
                    {
                        logger.Error($"Bucket {freshBucket.Id} has been marked as ready to delete but has no delete time", JobMasterLogSubjectType.Bucket, freshBucket.Id);
                        freshBucket.MarkAsLost();
                        await masterBucketsService.UpdateAsync(freshBucket);
                        continue;
                    }
                    
                    if (await agentJobsDispatcherService.HasJobsAsync(freshBucket.AgentConnectionId, freshBucket.Id))
                    {
                        logger.Warn($"Bucket {freshBucket.Id} has been marked as ready to delete but has jobs", JobMasterLogSubjectType.Bucket, freshBucket.Id);
                        freshBucket.MarkAsLost();
                        await masterBucketsService.UpdateAsync(freshBucket);
                        continue;
                    }

                    if (freshBucket.DeletesAt.Value > nowUtc)
                    {
                        continue;
                    }

                    logger.Info($"Destroying bucket {freshBucket.Id}", JobMasterLogSubjectType.Bucket, freshBucket.Id);
                    await masterBucketsService.DestroyAsync(freshBucket.Id);
                }
                finally
                {
                    masterDistributedLockerService.ReleaseLock(lockKeys.BucketLock(bucket.Id), bucketLockToken);
                }
            }

            return OnTickResult.Success(this);
        }
        finally
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.BucketRunnerLock(), lockToken);
        }
    }

    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(30);
}
