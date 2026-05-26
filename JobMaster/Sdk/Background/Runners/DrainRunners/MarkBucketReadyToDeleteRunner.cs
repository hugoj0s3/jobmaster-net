using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

/// <summary>
/// Monitors a draining bucket and transitions it to <c>ReadyToDelete</c> once it has
/// been idle (no jobs in the dispatcher) for at least <c>BucketNoJobsBeforeReadyToDelete</c>.
/// Skips immediately if no bucket ID is set or the bucket is not in <c>Draining</c> status.
/// Calls <c>ReadyToDelete()</c> on the bucket model and stops itself after the transition.
/// Runs every <see cref="SucceedInterval"/>.
/// </summary>
internal class MarkBucketReadyToDeleteRunner : BucketAwareRunner
{
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IAgentJobsDispatcherService agentJobsDispatcherService;

    private DateTime? noJobsSinceUtc;

    public override TimeSpan SucceedInterval => TimeSpan.FromMinutes(5);

    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(10);

    public MarkBucketReadyToDeleteRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
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

        var hasJobs = await agentJobsDispatcherService.HasJobsAsync(BackgroundAgentWorker.AgentConnectionId, BucketId!);
        if (hasJobs)
        {
            noJobsSinceUtc = null;
            return OnTickResult.Success(this);
        }

        noJobsSinceUtc ??= DateTime.UtcNow;

        if (DateTime.UtcNow.Subtract(noJobsSinceUtc.Value) < JobMasterConstants.BucketNoJobsBeforeReadyToDelete)
        {
            return OnTickResult.Success(this);
        }

        if (bucket.ReadyToDelete())
        {
            await masterBucketsService.UpdateAsync(bucket);
        }

        await StopAsync();
        return OnTickResult.Success(this);
    }
}
