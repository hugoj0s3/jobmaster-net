using JobMaster.Sdk.Contracts.Models.GenericRecords;

namespace JobMaster.Sdk.Contracts.Models.Jobs;

// Portable persistence DTO for repositories. Public setters for easy mapping.
public class JobPersistenceRecord
{
    public string ClusterId { get; set; } = string.Empty;
    public Guid Id { get; set; }
    public string JobDefinitionId { get; set; } = string.Empty;

    public int ScheduledType { get; set; }

    public string? BucketId { get; set; }
    public string? AgentConnectionId { get; set; }
    public string? AgentWorkerId { get; set; }

    public int Priority { get; set; }

    public DateTime OriginalScheduledAt { get; set; }
    public DateTime ScheduledAt { get; set; }

    public string MsgData { get; set; } = "{}";
    
    public GenericRecordEntry? Metadata { get; set; }

    public int Status { get; set; }

    public int NumberOfFailures { get; set; }

    public long TimeoutTicks { get; set; }

    public int MaxNumberOfRetries { get; set; }

    public DateTime CreatedAt { get; set; }
    public Guid? RecurringScheduleId { get; set; }

    public int? PartitionLockId { get; set; }
    public DateTime? PartitionLockExpiresAt { get; set; }

    public DateTime? ProcessDeadline { get; set; }

    public DateTime? ProcessingStartedAt { get; set; }

    public DateTime? SucceedExecutedAt { get; set; }
    
    public string? WorkerLane { get; set; }
    
    public string? Version { get; set; }
}
