namespace JobMaster.SqlBase.Models.Jobs;

// Flat persistence DTO for the concrete job_execution table.
// AgentConnectionId and HostId are stored as their IdValue strings.
internal class SqlJobExecutionPersistenceRecord
{
    public string ClusterId { get; set; } = string.Empty;
    public Guid Id { get; set; }
    public Guid JobId { get; set; }
    public DateTime StartedAt { get; set; }
    public string? AgentConnectionId { get; set; }
    public string? AgentWorkerId { get; set; }
    public string? BucketId { get; set; }
    public string? HostId { get; set; }
    public string? HostDisplayName { get; set; }
    public DateTime? FinalizedAt { get; set; }
    public string? OutcomeMessage { get; set; }
    public int Outcome { get; set; }
}
