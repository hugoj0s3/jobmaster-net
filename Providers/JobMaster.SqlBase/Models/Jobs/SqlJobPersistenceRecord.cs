using JobMaster.Sdk.Abstractions.Models.GenericRecords;

namespace JobMaster.SqlBase.Models.Jobs;

// Portable persistence DTO for SQL repositories. Public setters for easy mapping.
internal class SqlJobPersistenceRecord
{
    public string ClusterId { get; set; } = string.Empty;
    public Guid Id { get; set; }
    public string JobDefinitionId { get; set; } = string.Empty;

    public int TriggerSourceType { get; set; }

    public string? BucketId { get; set; }
    public string? AgentConnectionId { get; set; }
    public string? AgentWorkerId { get; set; }

    public string? HostId { get; set; }
    public string? HostDisplayName { get; set; }

    public int Priority { get; set; }

    public DateTime ScheduledAt { get; set; }
    public DateTime? NextPlanExecutionAt { get; set; }

    public string MsgData { get; set; } = "{}";

    /// <summary>
    /// JSON-serialized form of the job's metadata (already-serialized, straight from
    /// <see cref="JobMaster.Sdk.Abstractions.Models.Jobs.JobRawModel.Metadata"/> -- no transformation
    /// needed). Stored directly on the row so ordinary reads (Get/acquire/Query) don't need to LEFT
    /// JOIN the generic-record entry/value tables. Those tables remain the source of truth for
    /// <c>MetadataFilters</c> querying and are still written from <see cref="Metadata"/> below.
    /// </summary>
    public string? MetadataJson { get; set; }

    public GenericRecordEntry? Metadata { get; set; }

    public int Status { get; set; }

    public int NumberOfFailures { get; set; }

    public long TimeoutTicks { get; set; }

    public int MaxNumberOfRetries { get; set; }

    public DateTime CreatedAt { get; set; }
    public Guid? SourceId { get; set; }

    public Guid? PartitionLockId { get; set; }
    public DateTime? PartitionLockExpiresAt { get; set; }

    public DateTime? ProcessDeadline { get; set; }

    public DateTime? ProcessStartedAt { get; set; }

    public DateTime? FinalizedAt { get; set; }

    public string? WorkerLane { get; set; }

    public string? Version { get; set; }
}
