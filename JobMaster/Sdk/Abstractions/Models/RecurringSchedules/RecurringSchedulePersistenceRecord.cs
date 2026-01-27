using JobMaster.Sdk.Abstractions.Models.GenericRecords;

namespace JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

// Portable persistence DTO for repositories. Public setters for easy mapping.
internal class RecurringSchedulePersistenceRecord
{
    public string ClusterId { get; set; } = string.Empty;
    public Guid Id { get; set; }

    public string Expression { get; set; } = string.Empty;
    public string ExpressionTypeId { get; set; } = string.Empty;
    public string JobDefinitionId { get; set; } = string.Empty;

    public string? StaticDefinitionId { get; set; }
    public string? ProfileId { get; set; }

    public int Status { get; set; }
    public int RecurringScheduleType { get; set; }

    public DateTime? StaticDefinitionLastEnsured { get; set; }

    public DateTime? TerminatedAt { get; set; }

    public string MsgData { get; set; } = "{}";
    
    public GenericRecordEntry? Metadata { get; set; }

    public int? Priority { get; set; }
    public int? MaxNumberOfRetries { get; set; }
    public long? TimeoutTicks { get; set; }

    public string? BucketId { get; set; }
    public string? AgentConnectionId { get; set; }
    public string? AgentWorkerId { get; set; }

    public int? PartitionLockId { get; set; }
    public DateTime? PartitionLockExpiresAt { get; set; }

    public DateTime CreatedAt { get; set; }

    public DateTime? StartAfter { get; set; }
    public DateTime? EndBefore { get; set; }
    
    public DateTime? LastPlanCoverageUntil { get; set; }
    public DateTime? LastExecutedPlan { get; set; }
    public bool? HasFailedOnLastPlanExecution { get; set; }
    public bool? IsJobCancellationPending { get; set; }
    
    public string? WorkerLane { get; set; }
    
    public string? Version { get; set; }
}
