using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingRecurringSchedules;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Abstractions.Background.SavePendingJobs;

namespace JobMaster.Sdk.Background.Runners.SavePendingRecurringSchedule;

/// <summary>
/// Saves the <c>PendingSave</c> recurring schedules queued for a specific bucket on master.
/// Must be activated via <see cref="DefineBucketId"/> before ticking; skips immediately
/// if no bucket ID is set or the bucket is not in an Active or Completing state.
/// Each tick leases a batch and hands it to <see cref="RecurringScheduleSavePendingOperation.SaveAndAckAsync"/>;
/// a message not acked is leased again once its lease ends.
/// Runs every <see cref="SucceedInterval"/> while the bucket is healthy.
/// </summary>
internal class PollingSaveRecurringScheduleRunner : BucketAwareRunner, ISaveRecurringSchedulerRunner
{
    private readonly TimeSpan interval = TimeSpan.FromSeconds(2.5);

    private readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    private readonly IMasterBucketsService masterBucketsService;
    private readonly RecurringScheduleSavePendingOperation savePendingOperation;
    
    public PollingSaveRecurringScheduleRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        savePendingOperation = new RecurringScheduleSavePendingOperation(backgroundAgentWorker);
    }
    
    public override TimeSpan SucceedInterval => interval;

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (string.IsNullOrEmpty(BucketId))
        {
            return OnTickResult.Skipped(this);
        }
        
        var bucket = masterBucketsService.Get(BucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket == null || (bucket.Status != BucketStatus.Active && bucket.Status != BucketStatus.Completing))
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
            return OnTickResult.Skipped(TimeSpan.FromSeconds(1));
        }

        var saved = await savePendingOperation.SaveAndAckAsync(bucket, leasedSchedules);
        return saved ? OnTickResult.Success(this) : OnTickResult.Failed(this);
    }


    public void DefineBucketId(string bucketId)
    {
        if (string.IsNullOrEmpty(bucketId))
            throw new ArgumentNullException(nameof(bucketId));
        
        if (!string.IsNullOrEmpty(BucketId))
            throw new InvalidOperationException("BucketId is already defined.");
        
        this.BucketId = bucketId;
    }
}
