using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Api.ApiModels;

/// <summary>Represents an agent worker as returned by the API.</summary>
public class ApiAgentWorker : ApiClusterBaseModel
{
    /// <summary>Unique identifier of the worker.</summary>
    public string Id { get; set; } = string.Empty;
    /// <summary>Display name of the worker.</summary>
    public string Name { get; set; } = string.Empty;
    /// <summary>Identifier of the agent connection this worker belongs to, or <c>null</c> for Coordinator workers.</summary>
    public string? AgentConnectionId { get; set; }
    /// <summary>Name of the agent connection this worker belongs to, or <c>null</c> for Coordinator workers.</summary>
    public string? AgentConnectionName { get; set; }
    /// <summary>UTC timestamp when the worker was registered.</summary>
    public DateTime CreatedAt { get; set; }
    /// <summary>Whether the worker is currently alive (sending heartbeats).</summary>
    public bool IsAlive { get; set; }
    /// <summary>UTC timestamp when a stop was requested, if applicable.</summary>
    public DateTime? StopRequestedAt { get; set; }
    /// <summary>Grace period allowed for in-flight jobs to finish before the worker stops.</summary>
    public TimeSpan? StopGracePeriod { get; set; }
    /// <summary>UTC timestamp of the last heartbeat from this worker.</summary>
    public DateTime LastHeartbeat { get; set; }
    /// <summary>Operational mode of the worker.</summary>
    public AgentWorkerMode Mode { get; set; }
    /// <summary>Worker lane this worker is dedicated to, or <c>null</c> for the default lane.</summary>
    public string? WorkerLane { get; set; }
    /// <summary>Parallelism multiplier applied to this worker's execution capacity.</summary>
    public double ParallelismFactor { get; set; }
    /// <summary>Current operational status of the worker.</summary>
    public AgentWorkerStatus Status { get; set; }
    /// <summary>Identifier of the host running this worker.</summary>
    public string HostId { get; set; } = string.Empty;
    /// <summary>Display name of the host running this worker.</summary>
    public string HostDisplayName { get; set; } = string.Empty;
    
    internal static ApiAgentWorker FromDomain(AgentWorkerModel model)
    {
        return new ApiAgentWorker
        {
            ClusterId = model.ClusterId,
            Id = model.Id,
            Name = model.Name,
            AgentConnectionId = model.AgentConnectionId?.IdValue,
            AgentConnectionName = model.AgentConnectionId?.Name,
            CreatedAt = model.CreatedAt,
            IsAlive = model.IsAlive,
            StopRequestedAt = model.StopRequestedAt,
            StopGracePeriod = model.StopGracePeriod,
            LastHeartbeat = model.LastHeartbeat,
            Mode = model.Mode,
            WorkerLane = model.WorkerLane,
            ParallelismFactor = model.ParallelismFactor,
            Status = model.Status(),
            HostId = model.HostId?.IdValue ?? string.Empty,
            HostDisplayName = model.HostId?.HostDisplayName ?? string.Empty,
        };
    }
}