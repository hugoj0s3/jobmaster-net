using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Serialization;

namespace JobMaster.Sdk.Abstractions.Jobs;

/// <summary>
/// Centralized conversions among RecurringSchedule, RecurringScheduleRawModel, RecurringScheduleContext, and persistence records.
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

        if (!string.IsNullOrEmpty(raw.MsgData))
        {
            var dict = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(raw.MsgData);
            entity.MsgData = new MessageData(dict);
        }
        else
        {
            entity.MsgData = new MessageData();
        }
        
        if (!string.IsNullOrEmpty(raw.Metadata))
        {
            var metaDict = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(raw.Metadata!);
            entity.Metadata = new Metadata(metaDict);
        }
        else
        {
            entity.Metadata = new Metadata();
        }
        
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
            MsgData = InternalJobMasterSerializer.Serialize(s.MsgData.ToDictionary()),
            Metadata = s.Metadata != null ? InternalJobMasterSerializer.Serialize(s.Metadata?.ToDictionary()) : "{}",
            Priority = s.Priority,
            MaxNumberOfRetries = s.MaxNumberOfRetries,
            BucketId = s.BucketId,
            AgentConnectionId = s.AgentConnectionId,
            AgentWorkerId = s.AgentWorkerId,
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

    // Persistence helpers
    public static RecurringScheduleRawModel FromPersistence(RecurringSchedulePersistenceRecord d)
    {
        static DateTime Utc(DateTime dt) => DateTime.SpecifyKind(dt, DateTimeKind.Utc);
        static DateTime? UtcN(DateTime? dt) => dt.HasValue ? DateTime.SpecifyKind(dt.Value, DateTimeKind.Utc) : (DateTime?)null;

        var metadataDictionary = d.Metadata?.ToReadable().ToDictionary();
        var m = new RecurringScheduleRawModel(d.ClusterId)
        {
            Id = d.Id,
            Expression = d.Expression,
            ExpressionTypeId = d.ExpressionTypeId,
            JobDefinitionId = d.JobDefinitionId,
            StaticDefinitionId = d.StaticDefinitionId,
            ProfileId = d.ProfileId,
            Status = (RecurringScheduleStatus)d.Status,
            RecurringScheduleType = (RecurringScheduleType)d.RecurringScheduleType,
            StaticDefinitionLastEnsured = d.StaticDefinitionLastEnsured,
            TerminatedAt = UtcN(d.TerminatedAt),
            MsgData = string.IsNullOrEmpty(d.MsgData) ? "{}" : d.MsgData,
            Metadata =d.Metadata is null ? null : InternalJobMasterSerializer.Serialize(metadataDictionary),
            Priority = d.Priority.HasValue ? (JobMasterPriority?)d.Priority.Value : null,
            MaxNumberOfRetries = d.MaxNumberOfRetries,
            Timeout = d.TimeoutTicks.HasValue ? TimeSpan.FromTicks(d.TimeoutTicks.Value) : null,
            BucketId = d.BucketId,
            AgentConnectionId = d.AgentConnectionId != null ? new AgentConnectionId(d.AgentConnectionId) : null,
            AgentWorkerId = d.AgentWorkerId,
            PartitionLockId = d.PartitionLockId,
            PartitionLockExpiresAt = UtcN(d.PartitionLockExpiresAt),
            CreatedAt = Utc(d.CreatedAt),
            StartAfter = UtcN(d.StartAfter),
            EndBefore = UtcN(d.EndBefore),
            LastPlanCoverageUntil = UtcN(d.LastPlanCoverageUntil),
            LastExecutedPlan = UtcN(d.LastExecutedPlan),
            HasFailedOnLastPlanExecution = d.HasFailedOnLastPlanExecution,
            IsJobCancellationPending = d.IsJobCancellationPending,
            WorkerLane = d.WorkerLane,
            Version = d.Version,
        };

        return m;
    }

    public static RecurringSchedulePersistenceRecord ToPersistence(RecurringScheduleRawModel m)
    {
        
        var dictionary = string.IsNullOrEmpty(m.Metadata) ? new Dictionary<string, object?>() : InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(m.Metadata!);
        var writableMetadata = new Metadata(dictionary);
        var metadataEntry = GenericRecordEntry.FromWritableMetadata(m.ClusterId, MasterGenericRecordGroupIds.RecurringScheduleMetadata, m.Id.ToString("N"), writableMetadata);

        return new RecurringSchedulePersistenceRecord
        {
            ClusterId = m.ClusterId,
            Id = m.Id,
            Expression = m.Expression,
            ExpressionTypeId = m.ExpressionTypeId,
            JobDefinitionId = m.JobDefinitionId,
            StaticDefinitionId = m.StaticDefinitionId,
            ProfileId = m.ProfileId,
            Status = (int)m.Status,
            RecurringScheduleType = (int)m.RecurringScheduleType,
            StaticDefinitionLastEnsured = m.StaticDefinitionLastEnsured,
            TerminatedAt = m.TerminatedAt,
            MsgData = string.IsNullOrEmpty(m.MsgData) ? "{}" : m.MsgData,
            Metadata = metadataEntry,
            Priority = m.Priority.HasValue ? (int?)m.Priority.Value : null,
            MaxNumberOfRetries = m.MaxNumberOfRetries,
            TimeoutTicks = m.Timeout?.Ticks,
            BucketId = m.BucketId,
            AgentConnectionId = m.AgentConnectionId?.IdValue,
            AgentWorkerId = m.AgentWorkerId,
            PartitionLockId = m.PartitionLockId,
            PartitionLockExpiresAt = m.PartitionLockExpiresAt,
            CreatedAt = m.CreatedAt,
            StartAfter = m.StartAfter,
            EndBefore = m.EndBefore,
            LastPlanCoverageUntil = m.LastPlanCoverageUntil,
            LastExecutedPlan = m.LastExecutedPlan,
            HasFailedOnLastPlanExecution = m.HasFailedOnLastPlanExecution,
            IsJobCancellationPending = m.IsJobCancellationPending,
            WorkerLane = m.WorkerLane,
            Version = m.Version,
        };
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
            Metadata = !string.IsNullOrEmpty(recurringSchedule.Metadata) ? new Metadata(InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(recurringSchedule.Metadata ?? "{}")) : Metadata.Empty,
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
