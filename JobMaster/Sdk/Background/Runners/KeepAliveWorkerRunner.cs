using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

internal class KeepAliveWorkerRunner : JobMasterRunner
{
    private IMasterHeartbeatService masterHeartbeatService;
    private readonly TimeSpan interval = TimeSpan.FromSeconds(5);
    
    public override TimeSpan SucceedInterval => interval;
    
    public KeepAliveWorkerRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: false)
    {
        masterHeartbeatService = backgroundAgentWorker.GetClusterAwareService<IMasterHeartbeatService>();
    }
    
    public override Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        try
        {
            masterHeartbeatService.Heartbeat(ResourceHeartbeatType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
            return Task.FromResult(OnTickResult.Success(this));
        }
        catch (Exception e)
        {
            return Task.FromResult(OnTickResult.Failed(this, e, "Heartbeat failed"));
        }
    }
}