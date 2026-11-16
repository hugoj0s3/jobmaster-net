using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingRecurringSchedules;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Abstractions.Background.SavePendingJobs;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

internal class PollingDrainSavePendingRecurringScheduleRunner : BucketAwareRunner, IDrainSavePendingRecurringScheduleRunner
{
    private readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    private readonly IMasterBucketsService masterBucketsService;
    private readonly RecurringScheduleSavePendingOperation savePendingOperation;

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(3);

    public PollingDrainSavePendingRecurringScheduleRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
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

        var leasedSchedules = await agentJobsDispatcherService.LeaseSavePendingRecurAsync(
            bucket.AgentConnectionId,
            BucketId!,
            BackgroundAgentWorker.BucketBufferSize,
            JobSavePendingOperation.LeaseFor(BackgroundAgentWorker.BucketBufferSize));

        if (leasedSchedules.Count <= 0)
        {
            return OnTickResult.Skipped(TimeSpan.FromMinutes(1));
        }

        var saved = await savePendingOperation.SaveAndAckAsync(bucket, leasedSchedules);
        return saved ? OnTickResult.Success(this) : OnTickResult.Failed(this);
    }
}
