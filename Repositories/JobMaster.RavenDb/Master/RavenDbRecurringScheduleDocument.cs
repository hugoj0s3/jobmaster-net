using JobMaster.Abstractions.Models;

namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbRecurringScheduleDocument
{
    public Guid ScheduleId { get; set; }
    public string ClusterId { get; set; } = string.Empty;
    public string Expression { get; set; } = string.Empty;
    public string ExpressionTypeId { get; set; } = string.Empty;
    public string JobDefinitionId { get; set; } = string.Empty;
    public string? StaticDefinitionId { get; set; }
    public string? ProfileId { get; set; }
    public RecurringScheduleStatus Status { get; set; }
    public RecurringScheduleType RecurringScheduleType { get; set; }
    public DateTime? StaticDefinitionLastEnsured { get; set; }
    public DateTime? TerminatedAt { get; set; }
    public string MsgData { get; set; } = "{}";
    public string? MetadataJson { get; set; }
    public IDictionary<string, object?> MetadataValues { get; set; } = new Dictionary<string, object?>();
    public IDictionary<string, DateTime> MetadataDateTimeValues { get; set; } = new Dictionary<string, DateTime>();
    public IDictionary<string, Guid> MetadataGuidValues { get; set; } = new Dictionary<string, Guid>();
    public IDictionary<string, decimal> MetadataDecimalValues { get; set; } = new Dictionary<string, decimal>();
    public JobMasterPriority? Priority { get; set; }
    public int? MaxNumberOfRetries { get; set; }
    public long? TimeoutTicks { get; set; }
    public string? BucketId { get; set; }
    public string? AgentConnectionId { get; set; }
    public string? AgentWorkerId { get; set; }
    public Guid? PartitionLockId { get; set; }
    public string? HostId { get; set; }
    public string? HostDisplayName { get; set; }
    public DateTime? PartitionLockExpiresAt { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? StartAfter { get; set; }
    public DateTime? EndBefore { get; set; }
    public DateTime? LastPlanCoverageUntil { get; set; }
    public DateTime? LastExecutedPlan { get; set; }
    public bool? HasFailedOnLastPlanExecution { get; set; }
    public bool? IsJobCancellationPending { get; set; }
    public string? WorkerLane { get; set; }
}
