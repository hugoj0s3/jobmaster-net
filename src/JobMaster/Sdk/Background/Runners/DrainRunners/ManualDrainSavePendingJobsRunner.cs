using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingJobs;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Background.Runners.SavePendingJobs;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

internal class ManualDrainJobsRunner : DrainJobsRunnerBase, IDrainSavePendingJobsRunner
{
    
    private int failedSavedCountConsecutive = 0;

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(3);
    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(2.5);
    
    protected SavePendingOperation? savePendingOperation;

    public ManualDrainJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
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

        var savingPendingJobs = await agentJobsDispatcherService
            .DequeueSavePendingJobsAsync(BackgroundAgentWorker.AgentConnectionId, BucketId!, BackgroundAgentWorker.BatchSize);

        if (!savingPendingJobs.Any())
        {
            return OnTickResult.Skipped(TimeSpan.FromMinutes(1));
        }

        bool hasFailed = false;
        foreach (var job in savingPendingJobs)
        {
            var result = await savePendingOperation.SaveDrainSavePendingWithSafeGuardAsync(job); 
            if (result != SaveDrainResultCode.Success && result != SaveDrainResultCode.Skipped)
            {
               hasFailed = true;
            }
        }

        if (hasFailed)
        {
            failedSavedCountConsecutive++;
            var secondsToWait = Math.Min(180, 60 + failedSavedCountConsecutive * 5);
            return OnTickResult.Skipped(TimeSpan.FromSeconds(secondsToWait));
        }

        failedSavedCountConsecutive = 0;
        return OnTickResult.Success(this);
    }
}
