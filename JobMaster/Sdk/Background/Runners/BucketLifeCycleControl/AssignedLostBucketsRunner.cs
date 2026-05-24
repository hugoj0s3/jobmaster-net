using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Background.Runners.BucketLifeCycleControl;

/// <summary>
/// Reassigns Lost buckets to alive workers within the same agent connection.
/// Queries all Lost buckets, finds alive workers in matching agent connections, and
/// calls <c>ReadyToDrain</c> on each eligible bucket. Drain-mode workers are preferred
/// (with a weighted probability) to consolidate drain activity. A per-bucket distributed
/// lock prevents concurrent reassignment across coordinator workers.
/// Runs every <see cref="SucceedInterval"/>.
/// </summary>
internal class AssignedLostBucketsRunner : JobMasterRunner
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

        var lockToken = masterDistributedLockerService.TryLock(lockKeys.BucketRunnerLock(), TimeSpan.FromMinutes(2.5));
        if (lockToken == null)
        {
            return OnTickResult.Locked(this);
        }

        try
        {
            var buckets = await masterBucketsService.QueryAllNoCacheAsync(BucketStatus.Lost);
            var lostBuckets = buckets.Where(b => b.Status == BucketStatus.Lost);

            var workers = await masterAgentWorkersService.QueryWorkersAsync(useCache: false);
            var workersAlive = workers.Where(x => x.Status() == AgentWorkerStatus.Active).ToList();

            foreach (var bucket in lostBuckets)
            {
                ct.ThrowIfCancellationRequested();

                var bucketLockToken = masterDistributedLockerService.TryLock(lockKeys.BucketLock(bucket.Id), TimeSpan.FromSeconds(10));
                if (bucketLockToken == null)
                {
                    continue;
                }

                try
                {
                    var workerToSelect = workersAlive
                        .Where(x => x.AgentConnectionId.IdValue == bucket.AgentConnectionId.IdValue)
                        .Where(x => x.Mode == AgentWorkerMode.Drain || x.Mode == AgentWorkerMode.Full) // Only drain and full workers can be assigned to lost buckets.
                        .ToList();

                    if (!workerToSelect.Any())
                    {
                        logger.Critical($"Worker not found for bucket {bucket.Id}", JobMasterLogCategory.Bucket, bucket.Id);
                        continue;
                    }

                    if (workerToSelect.Any(x => x.Mode == AgentWorkerMode.Drain) && JobMasterRandomUtil.GetBoolean(0.75))
                    {
                        workerToSelect = workerToSelect.Where(x => x.Mode == AgentWorkerMode.Drain).ToList();
                    }

                    var worker = workerToSelect.Random()!;

                    if (bucket.ReadyToDrain(worker.Id))
                    {
                        logger.Info($"Bucket {bucket.Id} is ready to drain", JobMasterLogCategory.Bucket, bucket.Id);
                        await masterBucketsService.UpdateAsync(bucket);
                    }
                }
                finally
                {
                    masterDistributedLockerService.ReleaseLock(lockKeys.BucketLock(bucket.Id), bucketLockToken);
                }
            }
        }
        finally
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.BucketRunnerLock(), lockToken);
        }

        return OnTickResult.Success(this);
    }

    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(10);
}