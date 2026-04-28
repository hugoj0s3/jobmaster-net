using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Api.ApiModels;

public class ApiJobExecution
{
    public string Id { get; set; } = string.Empty;
    public string JobId { get; set; } = string.Empty;
    public DateTime StartedAt { get; set; }
    
    public string? AgentConnectionId { get; set; }
    public string? AgentConnectionName { get; set; }
    public string? AgentWorkerId { get; set; }
    public string? BucketId { get; set; }
    public string? HostId { get; set; }
    public string? HostDisplayName { get; set; }
    
    public DateTime? FinalizedAt { get; set; }
    public string? OutcomeMessage { get; set; }
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
