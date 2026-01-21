using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

internal class WorkerStopCoordinatorRunner : JobMasterRunner
{
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    private readonly IMasterClusterConfigurationService masterClusterConfigurationService;
    
    private readonly JobMasterLockKeys lockKeys;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    
    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(30);
    
    public WorkerStopCoordinatorRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) 
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        masterClusterConfigurationService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
    }
    
    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (masterDistributedLockerService.IsLocked(lockKeys.WorkerImmediateStopLock(BackgroundAgentWorker.AgentWorkerId)) && !BackgroundAgentWorker.StopImmediatelyRequested)
        {
            await BackgroundAgentWorker.StopImmediatelyAsync();
            return OnTickResult.Success(TimeSpan.Zero); // Stop immediately
        }

        if (masterDistributedLockerService.IsLocked(lockKeys.WorkerGracefulStopLock(BackgroundAgentWorker.AgentWorkerId)) && !BackgroundAgentWorker.StopRequested)
        {
            BackgroundAgentWorker.RequestStop();
            return OnTickResult.Success(TimeSpan.Zero);
        }
        
        if (BackgroundAgentWorker.StopImmediatelyRequested)
        {
            return OnTickResult.Failed(this);
        }
        
        // Only run if stop has been requested
        if (!BackgroundAgentWorker.StopRequested)
        {
            return OnTickResult.Success(this);
        }
        
        // Check if enough time has passed since stop was requested
        var stopDeadline = BackgroundAgentWorker.StopRequestedAt!.Value.Add(BackgroundAgentWorker.StopGracePeriod!.Value);
        if (DateTime.UtcNow < stopDeadline)
        {
            return OnTickResult.Success(TimeSpan.FromSeconds(30));
        }
        
        // After friendly stop deadline, stop immediately.
        await BackgroundAgentWorker.StopImmediatelyAsync();
        return OnTickResult.Success(TimeSpan.FromSeconds(10));
    }

    public override async Task OnStopAsync()
    {
        await MarkAllBucketAsLostAsync();
        await base.OnStopAsync();
    }

    public override async Task OnErrorAsync(Exception ex, CancellationToken ct)
    {
        await MarkAllBucketAsLostAsync();
        await base.OnErrorAsync(ex, ct);
    }

    private async Task MarkAllBucketAsLostAsync()
    {
        var buckets = this.masterBucketsService.QueryAllNoCache();
        foreach (var bucket in buckets)
        {
            if (bucket.AgentWorkerId == BackgroundAgentWorker.AgentWorkerId)
            {
                if (bucket.Status != BucketStatus.Lost && bucket.Status != BucketStatus.Draining && bucket.Status != BucketStatus.ReadyToDrain)
                { 
                    await BackgroundAgentWorker.WorkerClusterOperations.MarkBucketAsLostAsync(bucket);
                }
            }
        }
        
        await base.OnStopAsync();    
    }
}
