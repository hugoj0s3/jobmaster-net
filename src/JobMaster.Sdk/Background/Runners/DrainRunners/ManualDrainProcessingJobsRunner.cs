using JobMaster.Contracts.Extensions;
using JobMaster.Contracts.Models;
using JobMaster.Sdk.Background.Runners.SavePendingJobs;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Background.Runners;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Services.Agent;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

public class ManualDrainProcessingJobsRunner : DrainJobsRunnerBase, IDrainProcessingJobsRunner
{
    private JobMasterLockKeys lockKeys;
    private IMasterJobsService masterJobsService;
    private IMasterDistributedLockerService masterDistributedLockerService;
    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(3);
    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(2.5);
    
    protected SavePendingOperation? savePendingOperation;

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
        
        if (savePendingOperation is null)
        {
            savePendingOperation = new SavePendingOperation(BackgroundAgentWorker, BucketId!);
        }
        
        var bucket = masterBucketsService.Get(BucketId.NotNull(), JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket == null || bucket.Status != BucketStatus.Draining)
        {
            return OnTickResult.Skipped(this);
        }

        var processingJobs = await agentJobsDispatcherService
            .DequeueToProcessingAsync(BackgroundAgentWorker.AgentConnectionId, BucketId.NotNull(), BackgroundAgentWorker.BatchSize, null);

        if (!processingJobs.Any())
        {
            return OnTickResult.Skipped(TimeSpan.FromMinutes(1));
        }

        bool hasFailed = false;
        foreach (var job in processingJobs)
        {
            var result = await savePendingOperation.SaveDrainProcessingAsync(job); 
            if (result != SaveDrainResultCode.Success && result != SaveDrainResultCode.Skipped)
            {
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
