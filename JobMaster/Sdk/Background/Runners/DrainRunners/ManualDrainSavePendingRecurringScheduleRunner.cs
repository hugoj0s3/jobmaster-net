using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingRecurringSchedules;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

internal class ManualDrainSavePendingRecurringScheduleRunner : BucketAwareRunner, IDrainSavePendingRecurringScheduleRunner
{
    private readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    private readonly IMasterBucketsService masterBucketsService;
    private readonly RecurringScheduleSavePendingOperation savePendingOperation;

    private int failedSavedCountConsecutive = 0;

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(3);

    public ManualDrainSavePendingRecurringScheduleRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        savePendingOperation = new RecurringScheduleSavePendingOperation(backgroundAgentWorker);
    }

    public void DefineBucketId(string bucketId)
    {
        BucketId = bucketId;
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

        // Only reached for Full/Drain modes, which always have an AgentConnectionId.
        var recurringSchedules = await agentJobsDispatcherService.PullSavePendingRecurAsync(
            BackgroundAgentWorker.AgentConnectionId!,
            BucketId!,
            BackgroundAgentWorker.BucketBufferSize);

        if (recurringSchedules.Count <= 0)
        {
            return OnTickResult.Skipped(TimeSpan.FromMinutes(1));
        }

        bool hasFailed = false;
        foreach (var recurringScheduleRawModel in recurringSchedules)
        {
            var result = await savePendingOperation.SaveDrainWithSafeGuardAsync(recurringScheduleRawModel);
            if (result == SaveDrainResultCode.Failed)
                hasFailed = true;
        }

        if (hasFailed)
        {
            failedSavedCountConsecutive++;
            var secondsToWait = Math.Min(60, 10 + failedSavedCountConsecutive * 5);
            return OnTickResult.Skipped(TimeSpan.FromSeconds(secondsToWait));
        }

        failedSavedCountConsecutive = 0;
        return OnTickResult.Success(this);
    }
}
