using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Jobs;

/// <summary>
/// Centralized conversions among RecurringSchedule, RecurringScheduleRawModel, and RecurringScheduleContext.
/// Persistence-record conversions live with each repository family instead (e.g.
/// JobMaster.SqlBase.Models.RecurringSchedules.SqlRecurringSchedulePersistenceConvertUtil).
/// </summary>

internal static class RecurringScheduleConvertUtil
{
    // Raw -> Entity
    public static RecurringSchedule ToRecurringSchedule(RecurringScheduleRawModel raw)
    {
        var entity = new RecurringSchedule(raw.ClusterId)
        {
            Id = raw.Id,
            RecurExpression = RecurrenceExprCompiler.Compile(raw.ExpressionTypeId, raw.Expression),
            JobDefinitionId = raw.JobDefinitionId,
            StaticDefinitionId = raw.StaticDefinitionId,
            ProfileId = raw.ProfileId,
            Status = raw.Status,
            RecurringScheduleType = raw.RecurringScheduleType,
            TerminatedAt = raw.TerminatedAt,
            LastPlanCoverageUntil = raw.LastPlanCoverageUntil,
            LastExecutedPlan = raw.LastExecutedPlan,
            HasFailedOnLastPlanExecution = raw.HasFailedOnLastPlanExecution,
            AgentConnectionId = raw.AgentConnectionId,
            AgentWorkerId = raw.AgentWorkerId,
            HostId = raw.HostId,
            Priority = raw.Priority,
            MaxNumberOfRetries = raw.MaxNumberOfRetries,
            Timeout = raw.Timeout,
            CreatedAt = raw.CreatedAt,
            StartAfter = raw.StartAfter,
            EndBefore = raw.EndBefore,
            BucketId = raw.BucketId,
            PartitionLockId = raw.PartitionLockId,
            PartitionLockExpiresAt = raw.PartitionLockExpiresAt,
            IsJobCancellationPending = raw.IsJobCancellationPending,
            WorkerLane = raw.WorkerLane,
            StaticDefinitionLastEnsured = raw.StaticDefinitionLastEnsured,
        };

        entity.MsgData = KeyValueBagUtil.DeserializeMessageData(raw.MsgData);
        entity.Metadata = KeyValueBagUtil.DeserializeMetadata(raw.Metadata);
        
        // Version is persisted at the raw/persistence layers; propagate to the entity when present
        entity.Version = raw.Version;

        return entity;
    }

    // Entity -> Raw
    public static RecurringScheduleRawModel ToRawModel(RecurringSchedule s)
    {
        var result = new RecurringScheduleRawModel(s.ClusterId)
        {
            Id = s.Id,
            Expression = s.RecurExpression.Expression,
            ExpressionTypeId = s.RecurExpression.ExpressionTypeId,
            JobDefinitionId = s.JobDefinitionId,
            StaticDefinitionId = s.StaticDefinitionId,
            ProfileId = s.ProfileId,
            Status = s.Status,
            RecurringScheduleType = s.RecurringScheduleType,
            MsgData = KeyValueBagUtil.Serialize(s.MsgData),
            Metadata = KeyValueBagUtil.Serialize(s.Metadata),
            Priority = s.Priority,
            MaxNumberOfRetries = s.MaxNumberOfRetries,
            BucketId = s.BucketId,
            AgentConnectionId = s.AgentConnectionId,
            AgentWorkerId = s.AgentWorkerId,
            HostId = s.HostId,
            Timeout = s.Timeout,
            CreatedAt = s.CreatedAt,
            StartAfter = s.StartAfter,
            EndBefore = s.EndBefore,
            LastPlanCoverageUntil = s.LastPlanCoverageUntil,
            LastExecutedPlan = s.LastExecutedPlan,
            HasFailedOnLastPlanExecution = s.HasFailedOnLastPlanExecution,
            TerminatedAt = s.TerminatedAt,
            PartitionLockId = s.PartitionLockId,
            PartitionLockExpiresAt = s.PartitionLockExpiresAt,
            IsJobCancellationPending = s.IsJobCancellationPending,
            WorkerLane = s.WorkerLane,
            StaticDefinitionLastEnsured = s.StaticDefinitionLastEnsured,
            Version = s.Version
        };
        
        return result;
    }

    public static RecurringScheduleContext ToContext(RecurringScheduleRawModel recurringSchedule)
    {
        return new RecurringScheduleContext
        {
            Id = recurringSchedule.Id,
            ClusterId = recurringSchedule.ClusterId,
            ProfileId = recurringSchedule.ProfileId,
            CreatedAt = recurringSchedule.CreatedAt,
            RecurringScheduleType = recurringSchedule.RecurringScheduleType,
            StaticDefinitionId = recurringSchedule.StaticDefinitionId,
            RecurExpression = RecurrenceExprCompiler.Compile(recurringSchedule.ExpressionTypeId, recurringSchedule.Expression),
            JobDefinitionId = recurringSchedule.JobDefinitionId,
            StartAfter = recurringSchedule.StartAfter,
            EndBefore = recurringSchedule.EndBefore,
            Metadata = KeyValueBagUtil.DeserializeMetadata(recurringSchedule.Metadata).ToReadable(),
            WorkerLane = recurringSchedule.WorkerLane,
        };
    }
    
    // Entity -> Context
    public static RecurringScheduleContext ToContext(RecurringSchedule s)
    {
        return new RecurringScheduleContext
        {
            Id = s.Id,
            ClusterId = s.ClusterId,
            ProfileId = s.ProfileId,
            CreatedAt = s.CreatedAt,
            RecurringScheduleType = s.RecurringScheduleType,
            StaticDefinitionId = s.StaticDefinitionId,
            RecurExpression = s.RecurExpression,
            JobDefinitionId = s.JobDefinitionId,
            StartAfter = s.StartAfter,
            EndBefore = s.EndBefore,
            Metadata = s.Metadata?.ToReadable() ?? Metadata.Empty,
            WorkerLane = s.WorkerLane,
        };
    }
}
