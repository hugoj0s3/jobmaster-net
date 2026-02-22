namespace JobMaster.Sdk.Abstractions.Models.Agents;

internal class AgentConnectionModel : JobMasterBaseModel
{
    public AgentConnectionModel(string clusterId) : base(clusterId)
    {
    }
    
    protected AgentConnectionModel()
    {
    }

    public AgentConnectionId Id { get; set; } = null!;
    public string Footprint { get; set; } = string.Empty;

    public DateTime CreatedAt { get; set; }
    public DateTime FootprintCreatedAt { get; set; }
    public DateTime? LastHeartbeatAt { get; set; }
    
    public string RepositoryTypeId { get; set; } = string.Empty;
    
    public bool IsAlive() => (LastHeartbeatAt ?? FootprintCreatedAt) > DateTime.UtcNow.AddMinutes(-5);
}