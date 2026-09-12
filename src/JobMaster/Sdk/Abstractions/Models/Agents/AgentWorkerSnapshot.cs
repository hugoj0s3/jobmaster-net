using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Hosts;

namespace JobMaster.Sdk.Abstractions.Models.Agents;

internal class AgentWorkerSnapshot : JobMasterBaseModel
{
    public AgentWorkerSnapshot(AgentWorkerModel model) : base(model.ClusterId)
    {
        Id = model.Id;
        Name = model.Name;
        HostId = model.HostId;
        AgentConnectionId = model.AgentConnectionId;
        CreatedAt = model.CreatedAt;
        Mode = model.Mode;
        WorkerLane = model.WorkerLane;
        ParallelismFactor = model.ParallelismFactor;
    }
    
    public string Id { get; private set; } = string.Empty;
    public string Name { get; private  set; } = string.Empty;
    public HostId HostId { get; private set; } = null!;
    public AgentConnectionId? AgentConnectionId { get; private set; }
    public DateTime CreatedAt { get; private  set; }
    public AgentWorkerMode Mode { get; private set; } = AgentWorkerMode.Full;

    public string? WorkerLane { get; private set; }

    public double ParallelismFactor { get; private set; } = 1;
}