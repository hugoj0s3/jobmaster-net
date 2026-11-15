using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

/// <summary>
/// Monitors distributed lock signals to coordinate immediate and graceful worker shutdown.
/// Detects an immediate-stop lock and calls <c>StopImmediatelyAsync</c>, or a graceful-stop
/// lock and calls <c>RequestStop</c>. Once a graceful stop has been requested and the grace
/// period has elapsed, forces an immediate stop. Runs every <see cref="SucceedInterval"/>.
/// </summary>
internal class WorkerStopCoordinatorRunner : JobMasterRunner
{
    private readonly JobMasterLockKeys lockKeys;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(30);

    public WorkerStopCoordinatorRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
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

}
