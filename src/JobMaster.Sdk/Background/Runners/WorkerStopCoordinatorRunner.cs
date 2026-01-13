using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Services.Agent;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

/// <summary>
/// Coordinates both friendly and immediate stop operations for workers via distributed locks.
/// This runner monitors distributed lock signals from external systems to initiate graceful or immediate shutdowns,
/// manages the safe destruction of empty buckets during friendly stop operations, and handles self-destruction
/// of its own worker when stop operations are requested.
/// </summary>
/// <remarks>
/// <para><strong>Execution Interval:</strong> 30 seconds (when no stop requested), varies during stop process</para>
/// <para><strong>Lifecycle:</strong> Global runner (useIndependentLifecycle: false, useSemaphore: true)</para>
/// <para><strong>Self-Destruction Capability:</strong></para>
/// <list type="bullet">
/// <item><strong>External Control:</strong> Other systems/workers can trigger this worker's shutdown by creating distributed locks</item>
/// <item><strong>Self-Monitoring:</strong> Continuously checks for stop signals targeting its own worker ID</item>
/// <item><strong>Graceful Self-Shutdown:</strong> Coordinates its own worker's death through proper cleanup sequences</item>
/// </list>
/// <para><strong>Stop Operations:</strong></para>
/// <list type="bullet">
/// <item><strong>Immediate Stop:</strong> Checks WorkerImmediateStopLock(AgentWorkerId) → calls StopImmediatelyAsync()</item>
/// <item><strong>Friendly Stop:</strong> Checks WorkerFriendlyStopLock(AgentWorkerId) → calls RequestStop()</item>
/// <item><strong>Bucket Destruction:</strong> Safely destroys empty buckets after TransientThreshold during friendly stop</item>
/// </list>
/// <para><strong>Safety Features:</strong></para>
/// <list type="bullet">
/// <item>Uses agentJobsDispatcherService.HasJob() to verify buckets are empty before destruction</item>
/// <item>Respects TransientThreshold grace period for job completion</item>
/// <item>Prevents duplicate stop operations with internal flags</item>
/// <item>Immediate stop takes precedence over friendly stop</item>
/// </list>
/// <para><strong>Architecture:</strong> Enables distributed worker management - external systems control worker lifecycles by creating distributed locks rather than direct method calls</para>
/// </remarks>
public class WorkerStopCoordinatorRunner : JobMasterRunner
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

        if (masterDistributedLockerService.IsLocked(lockKeys.WorkerFriendlyStopLock(BackgroundAgentWorker.AgentWorkerId)) && !BackgroundAgentWorker.StopRequested)
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
        var stopDeadline = BackgroundAgentWorker.StopRequestedAt!.Value.Add(JobMasterConstants.DurationToStopFriendly);
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
