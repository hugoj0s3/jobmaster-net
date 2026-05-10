using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

/// <summary>
/// Sends a heartbeat for the current <c>Host</c> and calls <c>UpdateStatsAsync</c> on
/// every tick, keeping the host registration alive and its runtime statistics up to date.
/// Runs every <see cref="SucceedInterval"/>.
/// </summary>
internal class KeepAliveHostRunner : JobMasterRunner
{
    private readonly IMasterHeartbeatService masterHeartbeatService;
    private readonly IMasterHostService masterHostService;

    public KeepAliveHostRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: false)
    {
        masterHeartbeatService = backgroundAgentWorker.GetClusterAwareService<IMasterHeartbeatService>();
        masterHostService = backgroundAgentWorker.GetClusterAwareService<IMasterHostService>();
    }

    public override Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        try
        {
            masterHeartbeatService.Heartbeat(ResourceHeartbeatType.Host, this.BackgroundAgentWorker.HostId.IdValue);
            masterHostService.UpdateStatsAsync(this.BackgroundAgentWorker.HostId.IdValue);
            
            return Task.FromResult(OnTickResult.Success(this));
        }
        catch (Exception e)
        {
            return Task.FromResult(OnTickResult.Failed(this, e, "Heartbeat failed"));
        }
    }
    
    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(5);
}