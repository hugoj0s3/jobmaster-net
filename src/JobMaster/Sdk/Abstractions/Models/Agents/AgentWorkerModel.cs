using System.ComponentModel;
using JobMaster.Abstractions.Models;
using JobMaster.Internals;

namespace JobMaster.Sdk.Abstractions.Models.Agents;

[EditorBrowsable(EditorBrowsableState.Never)]
public class AgentWorkerModel : JobMasterBaseModel
{
    public AgentWorkerModel(string clusterId) : base(clusterId)
    {
    }

    protected AgentWorkerModel()
    {
    }

    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public AgentConnectionId AgentConnectionId { get; set; } = null!;
    public DateTime CreatedAt { get; set; }
    public bool IsAlive { get; set; }

    public DateTime? StopRequestedAt { get; set; }
    public TimeSpan? StopGracePeriod { get; set; }

    public DateTime LastHeartbeat { get; set; }

    public AgentWorkerMode Mode { get; set; } = AgentWorkerMode.Standalone;

    public string? WorkerLane { get; set; }

    public double ParallelismFactor { get; set; } = 1;

    public override bool IsValid() => base.IsValid() && JobMasterStringUtils.IsValidForId(Name) && JobMasterStringUtils.IsValidForId(Id);

    public AgentWorkerStatus Status()
    {
        if (!IsAlive)
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