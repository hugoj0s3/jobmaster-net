using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Abstractions.Jobs;

/// <summary>
/// Centralized conversions among Job, JobRawModel, JobContext, and persistence records.
/// Prefer these helpers over scattered conversions.
/// </summary>
internal static class JobConvertUtil
{
    // JobRawModel -> Job
    public static Job ToJob(JobRawModel raw)
    {
        var job = new Job(raw.ClusterId)
        {
            AgentConnectionId = raw.AgentConnectionId,
            BucketId = raw.BucketId,
            JobDefinitionId = raw.JobDefinitionId,
            TriggerSourceType = raw.TriggerSourceType,
            Status = raw.Status,
            Id = raw.Id,
            ScheduledAt = raw.ScheduledAt,
            NextPlanExecutionAt = raw.NextPlanExecutionAt,
            Priority = raw.Priority,
            AgentWorkerId = raw.AgentWorkerId,
            MaxNumberOfRetries = raw.MaxNumberOfRetries,
            Timeout = raw.Timeout,
            NumberOfFailures = raw.NumberOfFailures,
            CreatedAt = raw.CreatedAt,
            SourceId = raw.SourceId,
            WorkerLane = raw.WorkerLane,
            PartitionLockId = raw.PartitionLockId,
            PartitionLockExpiresAt = raw.PartitionLockExpiresAt,
            ProcessDeadline = raw.ProcessDeadline,
            ProcessStartedAt = raw.ProcessStartedAt,
            FinalizedAt = raw.FinalizedAt,
            Version = raw.Version,
            HostId = raw.HostId,
        };

        job.MsgData = KeyValueBagUtil.DeserializeMessageData(raw.MsgData);
        job.Metadata = KeyValueBagUtil.DeserializeMetadata(raw.Metadata);

        return job;
    }

    // Job -> JobRawModel
    public static JobRawModel ToJobRawModel(Job job)
    {
        return new JobRawModel(job.ClusterId)
        {
            AgentConnectionId = job.AgentConnectionId,
            BucketId = job.BucketId,
            JobDefinitionId = job.JobDefinitionId,
            TriggerSourceType = job.TriggerSourceType,
            Status = job.Status,
            Id = job.Id,
            ScheduledAt = job.ScheduledAt,
            NextPlanExecutionAt = job.NextPlanExecutionAt,
            Priority = job.Priority,
            AgentWorkerId = job.AgentWorkerId,
            MaxNumberOfRetries = job.MaxNumberOfRetries,
            Timeout = job.Timeout,
            NumberOfFailures = job.NumberOfFailures,
            MsgData = KeyValueBagUtil.Serialize(job.MsgData),
            Metadata = KeyValueBagUtil.Serialize(job.Metadata),
            CreatedAt = job.CreatedAt,
            SourceId = job.SourceId,
            WorkerLane = job.WorkerLane,
            PartitionLockId = job.PartitionLockId,
            PartitionLockExpiresAt = job.PartitionLockExpiresAt,
            ProcessDeadline = job.ProcessDeadline,
            ProcessStartedAt = job.ProcessStartedAt,
            FinalizedAt = job.FinalizedAt,
            Version = job.Version,
            HostId = job.HostId,
        };
    }

    // JobRawModel -> JobContext
    public static JobContext ToJobContext(JobRawModel raw)
    {
        var job = ToJob(raw);
        return ToJobContext(job);
    }

    // Job -> JobContext
    public static JobContext ToJobContext(Job job)
    {
        return new JobContext
        {
            Id = job.Id,
            ClusterId = job.ClusterId,
            JobDefinitionId = job.JobDefinitionId,
            TriggerSourceType = job.TriggerSourceType,
            Priority = job.Priority,
            Timeout = job.Timeout,
            MaxNumberOfRetries = job.MaxNumberOfRetries,
            ScheduledAt = job.ScheduledAt,
            CreatedAt = job.CreatedAt,
            SourceId = job.SourceId,
            Metadata = job.Metadata?.ToReadable() ?? Metadata.Empty,
            MsgData = job.MsgData.ToReadable(),
            WorkerLane = job.WorkerLane,
        };
    }

    // Persistence helpers (centralized here)
    public static JobRawModel FromPersistence(JobPersistenceRecord d)
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

    public static JobPersistenceRecord ToPersistence(JobRawModel m)
    {
        IWritableMetadata metadata = KeyValueBagUtil.DeserializeMetadata(m.Metadata);
        GenericRecordEntry metadataEntry = GenericRecordEntry.FromWritableMetadata(m.ClusterId, MasterGenericRecordGroupIds.JobMetadata, m.Id.ToString("N"), metadata);
        
        return new JobPersistenceRecord
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
}
