using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

internal class KeepAliveAgentConnectionRunner : JobMasterRunner
{
    private readonly IMasterHeartbeatService masterHeartbeatService;

    public KeepAliveAgentConnectionRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: false)
    {
        masterHeartbeatService = backgroundAgentWorker.GetClusterAwareService<IMasterHeartbeatService>();
    }

    public override Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        try
        {
            masterHeartbeatService.Heartbeat(
                ResourceHeartbeatType.AgentConnection, 
                this.BackgroundAgentWorker.AgentConnectionId.IdValue);
            return Task.FromResult(OnTickResult.Success(this));
        }
        catch (Exception e)
        {
            return Task.FromResult(OnTickResult.Failed(this, e, "Heartbeat failed"));
        }
    }
    
    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(5);
}