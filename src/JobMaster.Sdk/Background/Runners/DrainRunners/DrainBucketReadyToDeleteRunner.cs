using JobMaster.Contracts.Extensions;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Services.Agent;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

public class DrainBucketReadyToDeleteRunner : BucketAwareRunner
{
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IAgentJobsDispatcherService agentJobsDispatcherService;

    private DateTime? noJobsSinceUtc;

    public override TimeSpan SucceedInterval => TimeSpan.FromMinutes(5);

    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(10);

    public DrainBucketReadyToDeleteRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
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

        var bucket = masterBucketsService.Get(BucketId.NotNull(), JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket == null || bucket.Status != BucketStatus.Draining)
        {
            return OnTickResult.Skipped(this);
        }

        var hasJobs = await agentJobsDispatcherService.HasJobsAsync(BackgroundAgentWorker.AgentConnectionId, BucketId.NotNull());
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
