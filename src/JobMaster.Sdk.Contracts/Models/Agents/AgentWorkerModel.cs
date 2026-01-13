using JobMaster.Contracts.Models;
using JobMaster.Contracts.Utils;

namespace JobMaster.Sdk.Contracts.Models.Agents;

public class AgentWorkerModel : JobMasterBaseModel
{
    public AgentWorkerModel(string clusterId) : base(clusterId)
    {
    }
    
    protected AgentWorkerModel() {}

    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public AgentConnectionId AgentConnectionId { get; set; } = null!;
    public DateTime CreatedAt { get; set; }
    public bool IsAlive { get; set; }
    public DateTime LastHeartbeat { get; set; }
    
    public AgentWorkerMode Mode { get; set; } = AgentWorkerMode.Standalone;
    
    public string? WorkerLane { get; set; }
    
    public override bool IsValid() => base.IsValid() && JobMasterStringUtils.IsValidForId(Name) && JobMasterStringUtils.IsValidForId(Id);
}