using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingJobs;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Background.Runners.SavePendingJobs;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

/// <summary>
/// Saves the <c>PendingSave</c> jobs of a draining bucket on master, all held on master, and acks their messages
/// (<see cref="JobSavePendingOperation.SaveAndAckAsync"/>). Skips if no bucket ID is set or the bucket is not in
/// <c>Draining</c> status; a message not acked is leased again once its lease ends.
/// Runs every <see cref="SucceedInterval"/>.
/// </summary>
internal class PollingDrainSavePendingJobsRunner : DrainJobsRunnerBase, IDrainSavePendingJobsRunner
{
    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(3);
    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(2.5);

    protected JobSavePendingOperation? savePendingOperation;

    public PollingDrainSavePendingJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
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
            savePendingOperation = new JobSavePendingOperation(BackgroundAgentWorker, BucketId!);
        }
        
        var bucket = masterBucketsService.Get(BucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        
        if (bucket == null || bucket.Status != BucketStatus.Draining)
        {
            return OnTickResult.Skipped(this);
        }

        var leasedJobs = await agentJobsDispatcherService.LeaseSavePendingJobsAsync(
            bucket.AgentConnectionId,
            BucketId!,
            BackgroundAgentWorker.BucketBufferSize,
            JobSavePendingOperation.LeaseFor(BackgroundAgentWorker.BucketBufferSize));

        if (leasedJobs.Count <= 0)
        {
            return OnTickResult.Skipped(TimeSpan.FromMinutes(1));
        }

        // A draining bucket takes no new work: every job is held on master (no bucket assignments).
        foreach (var job in leasedJobs.Select(x => x.Job))
        {
            job.MarkAsHeldOnMaster();
        }

        var saved = await savePendingOperation.SaveAndAckAsync(bucket, leasedJobs, new Dictionary<Guid, BucketModel>());
        return saved ? OnTickResult.Success(this) : OnTickResult.Failed(this);
    }
}
