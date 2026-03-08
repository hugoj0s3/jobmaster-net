using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Api.ApiModels;

public class ApiAgentConnection
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string RepositoryTypeId { get; set; } = string.Empty;
    public string Footprint { get; set; } = string.Empty;
    
    public DateTime? LastHeartbeat { get; set; }
    public DateTime CreatedAt { get; set; }
    public bool ProtectConnectionChanges { get; set; }
    public bool IsAlive { get; set; }

    internal static ApiAgentConnection FromDomain(AgentConnectionModel agentConnection)
    {
        return new ApiAgentConnection
        {
            Id = agentConnection.Id.IdValue,
            Name = agentConnection.Id.Name,
            RepositoryTypeId = agentConnection.RepositoryTypeId,
            Footprint = agentConnection.Footprint,
            LastHeartbeat = agentConnection.LastHeartbeatAt,
            CreatedAt = agentConnection.CreatedAt,
            IsAlive = agentConnection.IsAlive(),
            ProtectConnectionChanges = agentConnection.ProtectConnectionChanges,
        };
    }
}