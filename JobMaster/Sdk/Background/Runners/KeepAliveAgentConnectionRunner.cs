using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

/// <summary>
/// Sends a heartbeat for the current <c>AgentConnection</c> on every tick, keeping its
/// registration alive in the master store so other runners can detect it as active.
/// Runs every <see cref="SucceedInterval"/>.
/// </summary>
internal class KeepAliveAgentConnectionRunner : AgentConnectionAwareRunner
{
    private readonly IMasterHeartbeatService masterHeartbeatService;

    public KeepAliveAgentConnectionRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker, AgentConnectionId agentConnectionId)
        : base(backgroundAgentWorker, agentConnectionId, bucketAwareLifeCycle: false, useSemaphore: false)
    {
        masterHeartbeatService = backgroundAgentWorker.GetClusterAwareService<IMasterHeartbeatService>();
    }

    public override Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        try
        {
            masterHeartbeatService.Heartbeat(
                ResourceHeartbeatType.AgentConnection,
                this.AgentConnectionId.IdValue);
            return Task.FromResult(OnTickResult.Success(this));
        }
        catch (Exception e)
        {
            return Task.FromResult(OnTickResult.Failed(this, e, "Heartbeat failed"));
        }
    }
    
    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(5);
}