using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Models.Agents;

internal class AgentWorkerModel : JobMasterBaseModel
{
    public AgentWorkerModel(string clusterId) : base(clusterId)
    {
    }

    protected AgentWorkerModel()
    {
    }

    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public HostId HostId { get; set; } = null!;
    public AgentConnectionId? AgentConnectionId { get; set; }
    public DateTime CreatedAt { get; set; }

    public DateTime? StopRequestedAt { get; set; }
    public TimeSpan? StopGracePeriod { get; set; }

    public DateTime LastHeartbeat { get; set; }

    public bool IsAlive() => LastHeartbeat > DateTime.UtcNow - JobMasterConstants.ResourceAliveThreshold;

    public AgentWorkerMode Mode { get; set; } = AgentWorkerMode.Full;

    public string? WorkerLane { get; set; }

    public double ParallelismFactor { get; set; } = 1;

    public override bool IsValid() => base.IsValid() && JobMasterStringUtils.IsValidForId(Name) && JobMasterStringUtils.IsValidForId(Id);

    public AgentWorkerStatus Status()
    {
        if (!IsAlive())
        {
            return AgentWorkerStatus.Dead;
        }
        
        if (StopRequestedAt.HasValue)
        {
            return AgentWorkerStatus.Stopping;
        }
        
        return AgentWorkerStatus.Active;
    }

}