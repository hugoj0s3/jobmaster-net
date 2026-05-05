using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

internal class ManualDrainProcessingJobsRunner : DrainJobsRunnerBase, IDrainProcessingJobsRunner
{
    private JobMasterLockKeys lockKeys;
    private IMasterJobsService masterJobsService;
    private IMasterDistributedLockerService masterDistributedLockerService;
    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(3);
    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(2.5);
    
    public ManualDrainProcessingJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        lockKeys = new JobMasterLockKeys(this.BackgroundAgentWorker.ClusterConnConfig.ClusterId);
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (string.IsNullOrEmpty(BucketId))
        {
            return OnTickResult.Skipped(this);
        }
        
        var bucket = masterBucketsService.Get(BucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket == null || bucket.Status != BucketStatus.Draining)
        {
            return OnTickResult.Skipped(this);
        }

        var processingJobs = await agentJobsDispatcherService
            .PullForProcessingAsync(BackgroundAgentWorker.AgentConnectionId, BucketId!, BackgroundAgentWorker.BucketBufferSize, null);

        if (!processingJobs.Any())
        {
            return OnTickResult.Skipped(TimeSpan.FromMinutes(1));
        }

        var activeIds = processingJobs
            .Where(j => !j.ExceedProcessDeadline())
            .Select(j => j.Id)
            .ToList();

        var exceededJobs = processingJobs
            .Where(j => j.ExceedProcessDeadline())
            .ToList();

        bool hasFailed = false;
        foreach (var partition in activeIds.Partition(JobMasterConstants.MaxBatchSizeForBulkOperation))
        {
            try
            {
                await masterJobsService.BulkUpdateAsync(BulkJobUpdateRequest.HeldOnMaster(partition.ToList()));
            }
            catch (Exception e)
            {
                logger.Error($"Drain: failed to bulk mark jobs as HeldOnMaster. PartitionSize={partition.Count}", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId, exception: e);
                hasFailed = true;
            }
        }

        foreach (var job in exceededJobs)
        {
            logger.Debug($"Drain skipping exceeded-deadline job. Recovery delegated to deadline runner. JobId={job.Id} ProcessDeadline={job.ProcessDeadline:O}", JobMasterLogSubjectType.Job, job.Id);
        }

        if (hasFailed)
        {
            return OnTickResult.Skipped(TimeSpan.FromSeconds(15));
        }

        return OnTickResult.Success(this);
    }
}
