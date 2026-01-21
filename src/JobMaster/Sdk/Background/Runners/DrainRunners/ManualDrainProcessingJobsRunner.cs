using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingJobs;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Runners.SavePendingJobs;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

internal class ManualDrainProcessingJobsRunner : DrainJobsRunnerBase, IDrainProcessingJobsRunner
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
        
        var bucket = masterBucketsService.Get(BucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket == null || bucket.Status != BucketStatus.Draining)
        {
            return OnTickResult.Skipped(this);
        }

        var processingJobs = await agentJobsDispatcherService
            .DequeueToProcessingAsync(BackgroundAgentWorker.AgentConnectionId, BucketId!, BackgroundAgentWorker.BatchSize, null);

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
