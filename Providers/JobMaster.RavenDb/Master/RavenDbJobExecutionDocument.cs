using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbJobExecutionDocument
{
    // Not named "Id" -- same identity-property collision as everywhere else in this provider.
    public Guid ExecutionId { get; set; }
    public string ClusterId { get; set; } = string.Empty;
    public Guid JobId { get; set; }
    public DateTime StartedAt { get; set; }
    public string? AgentConnectionId { get; set; }
    public string? AgentWorkerId { get; set; }
    public string? BucketId { get; set; }
    public string? HostId { get; set; }
    public string? HostDisplayName { get; set; }
    public DateTime? FinalizedAt { get; set; }
    public string? OutcomeMessage { get; set; }
    public JobExecutionOutcomeStatus Outcome { get; set; }
}
