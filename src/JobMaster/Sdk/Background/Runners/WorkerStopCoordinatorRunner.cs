using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Cancellation;

namespace JobMaster.Sdk.Background.Runners;

/// <summary>
/// Monitors distributed lock signals to coordinate a worker's two stops (terms in
/// <see cref="JobMasterStopControlFactory"/>). Detects an immediate-stop lock and calls <c>StopImmediatelyAsync</c>,
/// or a graceful-stop lock and calls <c>RequestGracefulStop</c>. Once a graceful stop's period has elapsed, forces an
/// immediate stop. Runs every <see cref="SucceedInterval"/>.
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

        if (masterDistributedLockerService.IsLocked(lockKeys.WorkerGracefulStopLock(BackgroundAgentWorker.AgentWorkerId)) && !BackgroundAgentWorker.GracefulStopRequested)
        {
            BackgroundAgentWorker.RequestGracefulStop();
            return OnTickResult.Success(TimeSpan.Zero);
        }
        
        if (BackgroundAgentWorker.StopImmediatelyRequested)
        {
            return OnTickResult.Failed(this);
        }
        
        // Only run if stop has been requested
        if (!BackgroundAgentWorker.GracefulStopRequested)
        {
            return OnTickResult.Success(this);
        }
        
        // Check if enough time has passed since stop was requested
        var stopDeadline = BackgroundAgentWorker.GracefulStopRequestedAt!.Value.Add(BackgroundAgentWorker.GracefulStopPeriod!.Value);
        if (DateTime.UtcNow < stopDeadline)
        {
            return OnTickResult.Success(TimeSpan.FromSeconds(30));
        }
        
        // After friendly stop deadline, stop immediately.
        await BackgroundAgentWorker.StopImmediatelyAsync();
        return OnTickResult.Success(TimeSpan.FromSeconds(10));
    }

}
