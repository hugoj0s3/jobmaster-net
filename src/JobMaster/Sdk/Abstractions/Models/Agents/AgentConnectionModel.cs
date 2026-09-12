using JobMaster.Sdk.Abstractions;

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
    public string Fingerprint { get; set; } = string.Empty;

    public DateTime CreatedAt { get; set; }
    public DateTime FingerprintCreatedAt { get; set; }
    public DateTime LastHeartbeatAt { get; set; }

    public string RepositoryTypeId { get; set; } = string.Empty;

    public bool ProtectConnectionChanges { get; set; }

    public bool IsAlive() => LastHeartbeatAt > DateTime.UtcNow - JobMasterConstants.ResourceAliveThreshold;
}