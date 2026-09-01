using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.SqlBase.Models.Jobs;

/// <summary>
/// Conversions between JobRawModel/JobExecution and the SQL-owned persistence DTOs. Each repository
/// family owns its own persistence-shape transform -- this one is SQL's.
/// </summary>
internal static class SqlJobPersistenceConvertUtil
{
    public static JobRawModel FromPersistence(SqlJobPersistenceRecord d)
    {
        var m = new JobRawModel(d.ClusterId)
        {
            Id = d.Id,
            JobDefinitionId = d.JobDefinitionId,
            TriggerSourceType = (JobMasterTriggerSourceType)d.TriggerSourceType,
            BucketId = d.BucketId,
            AgentConnectionId = d.AgentConnectionId != null ? new AgentConnectionId(d.AgentConnectionId) : null,
            AgentWorkerId = d.AgentWorkerId,
            Priority = (JobMasterPriority)d.Priority,
            ScheduledAt = d.ScheduledAt.AsUtc(),
            NextPlanExecutionAt = d.NextPlanExecutionAt.AsUtc(),
            MsgData = string.IsNullOrEmpty(d.MsgData) ? "{}" : d.MsgData,
            Metadata = d.MetadataJson,
            Status = (JobMasterJobStatus)d.Status,
            NumberOfFailures = d.NumberOfFailures,
            Timeout = TimeSpan.FromTicks(d.TimeoutTicks),
            MaxNumberOfRetries = d.MaxNumberOfRetries,
            CreatedAt = d.CreatedAt.AsUtc(),
            SourceId = d.SourceId,
            PartitionLockId = d.PartitionLockId,
            PartitionLockExpiresAt = d.PartitionLockExpiresAt.AsUtc(),
            ProcessDeadline = d.ProcessDeadline.AsUtc(),
            ProcessStartedAt = d.ProcessStartedAt.AsUtc(),
            FinalizedAt = d.FinalizedAt.AsUtc(),
            WorkerLane = d.WorkerLane,
            Version = d.Version,
            HostId = !string.IsNullOrEmpty(d.HostId) && !string.IsNullOrEmpty(d.HostDisplayName)
                ? HostId.Recover(d.HostDisplayName!, d.HostId!)
                : null,
        };

        return m;
    }

    public static SqlJobPersistenceRecord ToPersistence(JobRawModel m)
    {
        IWritableMetadata metadata = KeyValueBagUtil.DeserializeMetadata(m.Metadata);
        GenericRecordEntry metadataEntry = GenericRecordEntry.FromWritableMetadata(m.ClusterId, MasterGenericRecordGroupIds.JobMetadata, m.Id.ToString("N"), metadata);

        return new SqlJobPersistenceRecord
        {
            ClusterId = m.ClusterId,
            Id = m.Id,
            JobDefinitionId = m.JobDefinitionId,
            TriggerSourceType = (int)m.TriggerSourceType,
            BucketId = m.BucketId,
            AgentConnectionId = m.AgentConnectionId?.IdValue,
            AgentWorkerId = m.AgentWorkerId,
            Priority = (int)m.Priority,
            ScheduledAt = m.ScheduledAt,
            NextPlanExecutionAt = m.NextPlanExecutionAt,
            MsgData = string.IsNullOrEmpty(m.MsgData) ? "{}" : m.MsgData,
            MetadataJson = string.IsNullOrEmpty(m.Metadata) ? null : m.Metadata,
            Metadata = metadataEntry,
            Status = (int)m.Status,
            NumberOfFailures = m.NumberOfFailures,
            TimeoutTicks = m.Timeout.Ticks,
            MaxNumberOfRetries = m.MaxNumberOfRetries,
            CreatedAt = m.CreatedAt,
            SourceId = m.SourceId,
            PartitionLockId = m.PartitionLockId,
            PartitionLockExpiresAt = m.PartitionLockExpiresAt,
            ProcessDeadline = m.ProcessDeadline,
            ProcessStartedAt = m.ProcessStartedAt,
            FinalizedAt = m.FinalizedAt,
            WorkerLane = m.WorkerLane,
            Version = m.Version,
            HostId = m.HostId?.IdValue,
            HostDisplayName = m.HostId?.HostDisplayName,
        };
    }

    public static JobExecution FromPersistence(SqlJobExecutionPersistenceRecord rec)
    {
        var ex = new JobExecution(rec.ClusterId);
        ex.Id = rec.Id;
        ex.JobId = rec.JobId;
        ex.StartedAt = rec.StartedAt;
        ex.AgentWorkerId = rec.AgentWorkerId;
        ex.BucketId = rec.BucketId;
        ex.FinalizedAt = rec.FinalizedAt;
        ex.OutcomeMessage = rec.OutcomeMessage;
        ex.Outcome = (JobExecutionOutcomeStatus)rec.Outcome;

        if (!string.IsNullOrEmpty(rec.AgentConnectionId))
        {
            ex.AgentConnectionId = new AgentConnectionId(rec.AgentConnectionId!);
        }

        if (!string.IsNullOrEmpty(rec.HostId))
        {
            ex.HostId = HostId.Recover(rec.HostDisplayName ?? string.Empty, rec.HostId!);
        }

        return ex;
    }
}
