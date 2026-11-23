using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.SqlBase.Models.RecurringSchedules;

/// <summary>
/// Conversions between RecurringScheduleRawModel and the SQL-owned persistence DTO. Each repository
/// family owns its own persistence-shape transform -- this one is SQL's.
/// </summary>
internal static class SqlRecurringSchedulePersistenceConvertUtil
{
    public static RecurringScheduleRawModel FromPersistence(SqlRecurringSchedulePersistenceRecord d)
    {
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
            StaticDefinitionLastEnsured = d.StaticDefinitionLastEnsured.AsUtc(),
            TerminatedAt = d.TerminatedAt.AsUtc(),
            MsgData = string.IsNullOrEmpty(d.MsgData) ? "{}" : d.MsgData,
            Metadata = d.MetadataJson,
            Priority = d.Priority.HasValue ? (JobMasterPriority?)d.Priority.Value : null,
            MaxNumberOfRetries = d.MaxNumberOfRetries,
            Timeout = d.TimeoutTicks.HasValue ? TimeSpan.FromTicks(d.TimeoutTicks.Value) : null,
            BucketId = d.BucketId,
            AgentConnectionId = d.AgentConnectionId != null ? new AgentConnectionId(d.AgentConnectionId) : null,
            AgentWorkerId = d.AgentWorkerId,
            PartitionLockId = d.PartitionLockId,
            HostId = d.HostId != null ? HostId.Recover(d.HostDisplayName ?? "", d.HostId) : null,
            PartitionLockExpiresAt = d.PartitionLockExpiresAt.AsUtc(),
            CreatedAt = d.CreatedAt.AsUtc(),
            StartAfter = d.StartAfter.AsUtc(),
            EndBefore = d.EndBefore.AsUtc(),
            LastPlanCoverageUntil = d.LastPlanCoverageUntil.AsUtc(),
            LastExecutedPlan = d.LastExecutedPlan.AsUtc(),
            HasFailedOnLastPlanExecution = d.HasFailedOnLastPlanExecution,
            IsJobCancellationPending = d.IsJobCancellationPending,
            WorkerLane = d.WorkerLane,
            Version = d.Version,
        };

        return m;
    }

    public static SqlRecurringSchedulePersistenceRecord ToPersistence(RecurringScheduleRawModel m)
    {
        var writableMetadata = KeyValueBagUtil.DeserializeMetadata(m.Metadata);
        var metadataEntry = GenericRecordEntry.FromWritableMetadata(m.ClusterId, MasterGenericRecordGroupIds.RecurringScheduleMetadata, m.Id.ToString("N"), writableMetadata);

        return new SqlRecurringSchedulePersistenceRecord
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
            MetadataJson = string.IsNullOrEmpty(m.Metadata) ? null : m.Metadata,
            Priority = m.Priority.HasValue ? (int?)m.Priority.Value : null,
            MaxNumberOfRetries = m.MaxNumberOfRetries,
            TimeoutTicks = m.Timeout?.Ticks,
            BucketId = m.BucketId,
            AgentConnectionId = m.AgentConnectionId?.IdValue,
            AgentWorkerId = m.AgentWorkerId,
            PartitionLockId = m.PartitionLockId,
            HostId = m.HostId?.IdValue,
            HostDisplayName = m.HostId?.HostDisplayName,
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
}
