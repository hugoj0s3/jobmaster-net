using JobMaster.Abstractions.Models;

namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbJobDocument
{
    // Deliberately not named "Id" -- collides with RavenDB's document-identity convention (same issue
    // found and fixed on RavenDbLogDocument.LogId). Version is deliberately NOT a field here either --
    // it's RavenDB's own change vector (session.Advanced.GetChangeVectorFor), read after every
    // load/query/write, never persisted as domain data.
    public Guid JobId { get; set; }
    public string ClusterId { get; set; } = string.Empty;
    public string JobDefinitionId { get; set; } = string.Empty;
    public JobMasterTriggerSourceType TriggerSourceType { get; set; }
    public Guid? SourceId { get; set; }
    public string? BucketId { get; set; }

    // AgentConnectionId/HostId are flattened to their string IdValue (+ HostDisplayName for HostId,
    // which IdValue alone doesn't carry) -- same flattening SQL's JobPersistenceRecord uses, reconstructed
    // via AgentConnectionId(string)/HostId.Recover(...) on read.
    public string? AgentConnectionId { get; set; }
    public string? AgentWorkerId { get; set; }
    public string? HostId { get; set; }
    public string? HostDisplayName { get; set; }

    public JobMasterPriority Priority { get; set; }
    public DateTime ScheduledAt { get; set; }
    public DateTime? NextPlanExecutionAt { get; set; }
    public string MsgData { get; set; } = "{}";

    // MetadataJson is the verbatim JobRawModel.Metadata string -- stored and returned as-is, never
    // decomposed/reconstructed, so it carries zero round-trip risk regardless of which scalar types it
    // encodes (int/long/short/byte/char/bool/double/decimal/datetime/guid/string -- KeyValueBagUtil's own
    // JSON encoding already handles all of these correctly, same as SQL's MetadataJson column relies on).
    // The typed buckets below are a SEPARATE, purely query-side projection built from the same data, used
    // only by MetadataFilters -- same dispatch/merge pattern as RavenDbGenericRecordDocument's Values.
    public string? MetadataJson { get; set; }
    public IDictionary<string, object?> MetadataValues { get; set; } = new Dictionary<string, object?>();
    public IDictionary<string, DateTime> MetadataDateTimeValues { get; set; } = new Dictionary<string, DateTime>();
    public IDictionary<string, Guid> MetadataGuidValues { get; set; } = new Dictionary<string, Guid>();
    public IDictionary<string, decimal> MetadataDecimalValues { get; set; } = new Dictionary<string, decimal>();

    public JobMasterJobStatus Status { get; set; }
    public int NumberOfFailures { get; set; }
    public long TimeoutTicks { get; set; }
    public int MaxNumberOfRetries { get; set; }
    public DateTime CreatedAt { get; set; }
    public Guid? PartitionLockId { get; set; }
    public DateTime? PartitionLockExpiresAt { get; set; }
    public DateTime? ProcessDeadline { get; set; }
    public DateTime? ProcessStartedAt { get; set; }
    public DateTime? FinalizedAt { get; set; }
    public string? WorkerLane { get; set; }
}
