using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Api.ApiModels;

/// <summary>Represents an agent connection as returned by the API.</summary>
public class ApiAgentConnection
{
    /// <summary>Unique identifier of the agent connection.</summary>
    public string Id { get; set; } = string.Empty;
    /// <summary>Logical name of the agent connection.</summary>
    public string Name { get; set; } = string.Empty;
    /// <summary>The repository/transport type identifier used by this connection.</summary>
    public string RepositoryTypeId { get; set; } = string.Empty;
    /// <summary>Fingerprint hash of the connection configuration, used to detect unauthorized changes.</summary>
    public string Fingerprint { get; set; } = string.Empty;
    /// <summary>UTC timestamp of the last heartbeat received from this connection.</summary>
    public DateTime? LastHeartbeat { get; set; }
    /// <summary>UTC timestamp when the connection was registered.</summary>
    public DateTime CreatedAt { get; set; }
    /// <summary>Whether the connection fingerprint is protected against configuration changes.</summary>
    public bool ProtectConnectionChanges { get; set; }
    /// <summary>Whether the connection is currently alive (i.e. sending heartbeats).</summary>
    public bool IsAlive { get; set; }

    internal static ApiAgentConnection FromDomain(AgentConnectionModel agentConnection)
    {
        return new ApiAgentConnection
        {
            Id = agentConnection.Id.IdValue,
            Name = agentConnection.Id.Name,
            RepositoryTypeId = agentConnection.RepositoryTypeId,
            Fingerprint = agentConnection.Fingerprint,
            LastHeartbeat = agentConnection.LastHeartbeatAt,
            CreatedAt = agentConnection.CreatedAt,
            IsAlive = agentConnection.IsAlive(),
            ProtectConnectionChanges = agentConnection.ProtectConnectionChanges,
        };
    }
}