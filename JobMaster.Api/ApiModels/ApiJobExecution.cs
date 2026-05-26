using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Api.ApiModels;

/// <summary>Represents a single execution attempt for a job as returned by the API.</summary>
public class ApiJobExecution
{
    /// <summary>Unique identifier of the execution record (base64url-encoded GUID).</summary>
    public string Id { get; set; } = string.Empty;
    /// <summary>Identifier of the job this execution belongs to (base64url-encoded GUID).</summary>
    public string JobId { get; set; } = string.Empty;
    /// <summary>UTC timestamp when execution started.</summary>
    public DateTime StartedAt { get; set; }
    /// <summary>Agent connection that handled this execution.</summary>
    public string? AgentConnectionId { get; set; }
    /// <summary>Name of the agent connection that handled this execution.</summary>
    public string? AgentConnectionName { get; set; }
    /// <summary>Worker that executed this job.</summary>
    public string? AgentWorkerId { get; set; }
    /// <summary>Bucket that owned the job during execution.</summary>
    public string? BucketId { get; set; }
    /// <summary>Host that ran the executing worker.</summary>
    public string? HostId { get; set; }
    /// <summary>Display name of the host that ran the executing worker.</summary>
    public string? HostDisplayName { get; set; }
    /// <summary>UTC timestamp when execution reached a final outcome, if applicable.</summary>
    public DateTime? FinalizedAt { get; set; }
    /// <summary>Optional message describing the execution outcome (e.g. error details).</summary>
    public string? OutcomeMessage { get; set; }
    /// <summary>String representation of the execution outcome status.</summary>
    public string Outcome { get; set; } = string.Empty;

    internal static ApiJobExecution FromDomain(JobExecution execution)
    {
        return new ApiJobExecution
        {
            Id = execution.Id.ToBase64(),
            JobId = execution.JobId.ToBase64(),
            StartedAt = execution.StartedAt,
            AgentConnectionId = execution.AgentConnectionId?.IdValue,
            AgentConnectionName = execution.AgentConnectionId?.Name,
            AgentWorkerId = execution.AgentWorkerId,
            BucketId = execution.BucketId,
            HostId = execution.HostId?.IdValue,
            HostDisplayName = execution.HostId?.HostDisplayName,
            FinalizedAt = execution.FinalizedAt,
            OutcomeMessage = execution.OutcomeMessage,
            Outcome = execution.Outcome.ToString(),
        };
    }
}
