using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Internals;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Jobs;

internal class RecurringSchedule : JobMasterBaseModel
{
    internal RecurringSchedule(string clusterId) : base(clusterId)
    {
        Id = JobMasterRandomUtil.NewGuid();
        RecurringScheduleType = RecurringScheduleType.Dynamic;
        CreatedAt = DateTime.UtcNow;
        Status = RecurringScheduleStatus.PendingSave;
        RecurExpression = new NeverRecursCompiledExpr();
        JobDefinitionId = string.Empty;
    }
    
    public Guid Id { get; internal set; }
    public IRecurrenceCompiledExpr RecurExpression { get; internal set; }
    public string JobDefinitionId { get; internal set; }
    public string? StaticDefinitionId { get; internal set; }
    public string? ProfileId { get; internal set; }
    public RecurringScheduleStatus Status { get; internal set; }
    public RecurringScheduleType RecurringScheduleType { get; internal set; }
    public IWriteableMessageData MsgData { get; internal set; } = new MessageData();
    public IWritableMetadata? Metadata { get; internal set; } = new Metadata();
    public JobMasterPriority? Priority { get; internal set; }
    public int? MaxNumberOfRetries { get; internal set; }
    public TimeSpan? Timeout { get; internal set; }
    public int? PartitionLockId { get; internal set; }
    public DateTime? PartitionLockExpiresAt { get; internal set; }
    public string? BucketId { get; internal set; }
    public AgentConnectionId? AgentConnectionId { get; internal set; }
    public string? AgentWorkerId { get; internal set; }
    
    public DateTime CreatedAt { get; internal set; }
    public DateTime? StartAfter { get; internal set; }
    public DateTime? EndBefore { get; internal set; }
    public DateTime? TerminatedAt { get; internal set; }
    
    public DateTime? LastPlanCoverageUntil { get; internal set; }
    public DateTime? LastExecutedPlan { get; internal set; }
    public bool? HasFailedOnLastPlanExecution { get; internal set; }
    
    public bool? IsJobCancellationPending { get; internal set; }
    
    public string? WorkerLane { get; internal set; }
    
    public DateTime? StaticDefinitionLastEnsured { get; internal set; }
    
    public string? Version { get; internal set; }
    
    public static RecurringSchedule New<T>(
        string clusterId,
        IWriteableMessageData? values, 
        IRecurrenceCompiledExpr expression, 
        JobMasterPriority? priority,
        TimeSpan? timeout, 
        int? maxNumberOfRetries, 
        IWritableMetadata? metadata,
        RecurringScheduleType recurringScheduleType,
        string? staticDefinitionId,
        DateTime? startAfter,
        DateTime? endBefore,
        string? workerLane) where T : IJobHandler
    {
        var jobDefinitionId =
            typeof(T).GetCustomAttributes(false).OfType<JobMasterDefinitionIdAttribute>().FirstOrDefault()?.JobDefinitionId ??
            typeof(T).FullName;
        
        if (string.IsNullOrEmpty(jobDefinitionId))
        {
            throw new InvalidOperationException($"JobDefinitionId was not resolved. " +
                                                $"try to add JobDefinitionIdAttribute on {typeof(T)}.");
        }

        return New(
            clusterId, 
            jobDefinitionId,
            values, 
            expression, 
            priority, 
            timeout, 
            maxNumberOfRetries, 
            metadata, 
            recurringScheduleType, 
            staticDefinitionId, 
            startAfter, 
            endBefore,
            workerLane);
    }

    public static RecurringSchedule New(
        string clusterId, 
        string jobDefinitionId,
        IWriteableMessageData? values, 
        IRecurrenceCompiledExpr expression, 
        JobMasterPriority? priority, 
        TimeSpan? timeout, 
        int? maxNumberOfRetries,
        IWritableMetadata? metadata, 
        RecurringScheduleType recurringScheduleType, 
        string? staticDefinitionId, 
        DateTime? startAfter, 
        DateTime? endBefore,
        string? workerLane)
    {
        if (recurringScheduleType == RecurringScheduleType.Dynamic)
        {
            staticDefinitionId = null;
        }

        return new RecurringSchedule(clusterId)
        {
            RecurExpression = expression,
            JobDefinitionId = jobDefinitionId,
            Status =  RecurringScheduleStatus.PendingSave,
            RecurringScheduleType = recurringScheduleType,
            MsgData = values ?? MessageData.Empty,
            Metadata = metadata ?? new Metadata(),
            Timeout = timeout,
            MaxNumberOfRetries = maxNumberOfRetries,
            Priority = priority,
            StaticDefinitionId = staticDefinitionId,
            StartAfter = startAfter,
            EndBefore = endBefore,
            WorkerLane = workerLane
        };
    }

    public RecurringScheduleRawModel ToModel()
    {
        return RecurringScheduleConvertUtil.ToRawModel(this);
    }
}