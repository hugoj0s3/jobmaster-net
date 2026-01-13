using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using JobMaster.Contracts.Extensions;
using JobMaster.Contracts.Models;
using JobMaster.Contracts.Models.Attributes;
using JobMaster.Contracts.RecurrenceExpressions;
using JobMaster.Contracts.StaticRecurringSchedules;
using JobMaster.Sdk.Contracts.Jobs;
using JobMaster.Sdk.Contracts.Models.Agents;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Serialization;

namespace JobMaster.Sdk.Contracts.Models.RecurringSchedules;

public class RecurringScheduleRawModel : JobMasterBaseModel
{
    public RecurringScheduleRawModel(string clusterId) : base(clusterId)
    {
    }

    [JsonConstructor]
    internal RecurringScheduleRawModel()
    {
    }

    [JsonInclude]
    public Guid Id { get; internal set; }
    
    [JsonInclude]
    public string Expression { get; internal set; } = string.Empty;
    
    [JsonInclude]
    public string ExpressionTypeId { get; internal set; } = NeverRecursExprCompiler.TypeId;
    
    [JsonInclude]
    public string JobDefinitionId { get; internal set; } = string.Empty;
    
    [JsonInclude]
    public string? StaticDefinitionId { get; internal set; }
    
    [JsonInclude]
    public string? ProfileId { get; internal set; }
    
    [JsonInclude]
    public RecurringScheduleStatus Status { get; internal set; }
    
    [JsonInclude]
    public RecurringScheduleType RecurringScheduleType { get; internal set; }
    
    [JsonInclude]
    public DateTime? TerminatedAt { get; internal set; }
    
    [JsonInclude]
    public string MsgData { get; internal set; } = "{}";
    
    [JsonInclude]
    public string? Metadata { get; internal set; }
    
    [JsonInclude]
    public JobMasterPriority? Priority { get; internal set; }
    
    [JsonInclude]
    public int? MaxNumberOfRetries { get; internal set; }
    
    [JsonInclude]
    public TimeSpan? Timeout { get; internal set; }
    
    [JsonInclude]
    public string? BucketId { get; internal set; }
    
    [JsonInclude]
    public AgentConnectionId? AgentConnectionId { get; internal set; }
    
    [JsonInclude]
    public string? AgentWorkerId { get; internal set; }
    
    [JsonInclude]
    public int? PartitionLockId { get; internal set; }
    
    [JsonInclude]
    public DateTime? PartitionLockExpiresAt { get; internal set; }
    
    [JsonInclude]
    public DateTime CreatedAt { get; internal set; }
    
    [JsonInclude]
    public DateTime? StartAfter { get; internal set; }
    
    [JsonInclude]
    public DateTime? EndBefore { get; internal set; }
    
    [JsonInclude]
    public DateTime? LastPlanCoverageUntil { get; internal set; }
    
    [JsonInclude]
    public DateTime? LastExecutedPlan { get; internal set; }
    
    [JsonInclude]
    public bool? HasFailedOnLastPlanExecution { get; internal set; }
    
    [JsonInclude]
    public bool? IsJobCancellationPending { get; internal set; }

    [JsonInclude]
    public DateTime? StaticDefinitionLastEnsured { get; internal set; }
    
    [JsonInclude]
    public string? WorkerLane { get; internal set; }
    
    [JsonInclude]
    public string? Version { get; internal set; }

    public void Active()
    {
        Status = RecurringScheduleStatus.Active;
        BucketId = null;
        AgentConnectionId = null;
        AgentWorkerId = null;
    }

    public void AssignPendingRecurringScheduleToBucket(AgentConnectionId agentConnectionId, string agentWorkerId, string bucketId)
    {
        Status = RecurringScheduleStatus.PendingSave;
        BucketId = bucketId;
        AgentConnectionId = agentConnectionId;
        AgentWorkerId = agentWorkerId;
    }

    public bool TryToCancel()
    {
        if (Status.IsFinalStatus())
        {
            return false;
        }
        
        Status = RecurringScheduleStatus.Canceled;
        TerminatedAt = DateTime.UtcNow;
        
        BucketId = null;
        AgentConnectionId = null;
        AgentWorkerId = null;
        
        IsJobCancellationPending = true;
        
        return true;
    }

    public void TryInactivate()
    {
        if (Status.IsFinalStatus())
        {
            return;
        }
        
        Status = RecurringScheduleStatus.Inactive;
        TerminatedAt = DateTime.UtcNow;
        
        BucketId = null;
        AgentConnectionId = null;
        AgentWorkerId = null;
        
        IsJobCancellationPending = false;
    }

    public void TryEnded(DateTime? terminatedAt = null)
    {
        if (Status.IsFinalStatus())
        {
            return;
        }
        
        Status = RecurringScheduleStatus.Inactive;
        TerminatedAt = terminatedAt ?? DateTime.UtcNow;
        
        BucketId = null;
        AgentConnectionId = null;
        AgentWorkerId = null;
        
        IsJobCancellationPending = false;
    }

    public int CalcEstimateByteSize()
    {
        return JobMasterRawMessage.CalcEstimateByteSize(this);
    }
    
    public void HasCancelJobsFinish()
    {
        IsJobCancellationPending = false;
    }

    private readonly TimeSpan staticIdleTimeSpan = TimeSpan.FromMinutes(5);
    public bool IsStaticIdle(DateTime? baseTime = null)
    {
        // Only static schedules can be idle - dynamic schedules are never idle
        if (RecurringScheduleType != RecurringScheduleType.Static)
        {
            return false;
        }
        
        // Static schedule is idle if not active or hasn't been kept alive recently
        if (Status != RecurringScheduleStatus.Active)
        {
            return true;
        }
        
        var afterAt = DateTime.UtcNow - staticIdleTimeSpan;
        if (baseTime.HasValue && afterAt < baseTime)
        {
            afterAt = baseTime.Value;
        }
        
        return StaticDefinitionLastEnsured < afterAt;
    }

    // Persistence mappers
    public static RecurringScheduleRawModel RecoverFromDb(RecurringSchedulePersistenceRecord d)
        => RecurringScheduleConvertUtil.FromPersistence(d);

    public static RecurringSchedulePersistenceRecord ToPersistence(RecurringScheduleRawModel m)
        => RecurringScheduleConvertUtil.ToPersistence(m);

    public void UpdateStaticFromDefinition(StaticRecurringScheduleDefinition definition)
    {
        if (string.IsNullOrEmpty(StaticDefinitionId))
        {
            throw new InvalidOperationException("StaticDefinitionId is null");
        }
        
        if (RecurringScheduleType != RecurringScheduleType.Static)
        {
            throw new InvalidOperationException("RecurringScheduleType is not Static");
        }
        
        if (definition.Id != StaticDefinitionId)
        {
            throw new ArgumentException("Invalid configId", nameof(definition));
        }
        
        ExpressionTypeId = definition.CompiledExpr.ExpressionTypeId;
        Expression = definition.CompiledExpr.Expression;
        JobDefinitionId = definition.JobDefinitionId;
        MaxNumberOfRetries = definition.MaxNumberOfRetries;
        Priority = definition.Priority;
        Timeout = definition.Timeout;
        StartAfter = definition.StartAfter;
        EndBefore = definition.EndBefore;
        
        Metadata = Metadata = definition.Metadata != null ? InternalJobMasterSerializer.Serialize(definition.Metadata?.ToDictionary()) : "{}";

        Status =  (EndBefore is null || EndBefore > DateTime.UtcNow) ? RecurringScheduleStatus.Active : RecurringScheduleStatus.Inactive;
        StaticDefinitionLastEnsured = DateTime.UtcNow;
        
        WorkerLane = definition.WorkerLane;
    }
}