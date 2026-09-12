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
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

internal class PollingDrainProcessingJobsRunner : DrainJobsRunnerBase, IDrainProcessingJobsRunner
{
    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(3);
    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(2.5);
    
    public PollingDrainProcessingJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
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
            .PullForProcessingAsync(bucket.AgentConnectionId, BucketId!, BackgroundAgentWorker.BucketBufferSize, null);

        if (!processingJobs.Any())
        {
            return OnTickResult.Skipped(TimeSpan.FromMinutes(1));
        }

        var jobIds = processingJobs.Select(j => j.Id).ToList();

        bool hasFailed = false;
        var maxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(BackgroundAgentWorker.ClusterConnConfig.ClusterId);
        foreach (var partition in jobIds.Partition(maxBatchSize))
        {
            try
            {
                await BackgroundAgentWorker.WorkerClusterOperations
                    .ExecWithRetryAsync(o => o.BulkUpdateAsync(BulkJobUpdateRequest.HeldOnMaster(partition.ToList())));
            }
            catch (Exception e)
            {
                logger.Error($"Drain: failed to bulk mark jobs as HeldOnMaster. PartitionSize={partition.Count}", JobMasterLogCategory.AgentWorker, BackgroundAgentWorker.AgentWorkerId, exception: e);
                hasFailed = true;
            }
        }

        if (hasFailed)
        {
            return OnTickResult.Skipped(TimeSpan.FromSeconds(15));
        }

        return OnTickResult.Success(this);
    }
}
