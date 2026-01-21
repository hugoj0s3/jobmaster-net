using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Background;

public abstract class BucketAwareRunner : JobMasterRunner, IBucketAwareRunner
{
    public string? BucketId { get; set; }
    protected BucketAwareRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : 
        base(backgroundAgentWorker, true, false)
    {
    }
    
    public override async Task OnStopAsync()
    {
        if (!string.IsNullOrEmpty(BucketId))
        {
            await BackgroundAgentWorker.WorkerClusterOperations.MarkBucketAsLostIfNotDrainingAsync(BucketId!);
        }
        
        await base.OnStopAsync();
    }

    public JobMasterClusterConnectionConfig ClusterConnConfig => this.BackgroundAgentWorker.ClusterConnConfig;
}