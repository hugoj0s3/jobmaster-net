using System.ComponentModel;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Serialization;

namespace JobMaster.Sdk.Abstractions.Jobs;

/// <summary>
/// Centralized conversions among Job, JobRawModel, JobContext, and persistence records.
/// Prefer these helpers over scattered conversions.
/// </summary>

[EditorBrowsable(EditorBrowsableState.Never)]
public static class JobConvertUtil
{
    // JobRawModel -> Job
    public static Job ToJob(JobRawModel raw)
    {
        var job = new Job(raw.ClusterId)
        {
            AgentConnectionId = raw.AgentConnectionId,
            BucketId = raw.BucketId,
            JobDefinitionId = raw.JobDefinitionId,
            ScheduleSourceType = raw.ScheduleSourceType,
            Status = raw.Status,
            Id = raw.Id,
            OriginalScheduledAt = raw.OriginalScheduledAt,
            ScheduledAt = raw.ScheduledAt,
            Priority = raw.Priority,
            AgentWorkerId = raw.AgentWorkerId,
            MaxNumberOfRetries = raw.MaxNumberOfRetries,
            Timeout = raw.Timeout,
            NumberOfFailures = raw.NumberOfFailures,
            CreatedAt = raw.CreatedAt,
            RecurringScheduleId = raw.RecurringScheduleId,
            WorkerLane = raw.WorkerLane,
            PartitionLockId = raw.PartitionLockId,
            PartitionLockExpiresAt = raw.PartitionLockExpiresAt,
            ProcessDeadline = raw.ProcessDeadline,
            ProcessingStartedAt = raw.ProcessingStartedAt,
            SucceedExecutedAt = raw.SucceedExecutedAt,
            Version = raw.Version
        };

        if (!string.IsNullOrEmpty(raw.MsgData))
        {
            var values = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(raw.MsgData)
                         ?? new Dictionary<string, object?>();
            job.MsgData = new MessageData(values);
        }
        else
        {
            job.MsgData = new MessageData();
        }
        
        if (!string.IsNullOrEmpty(raw.Metadata))
        {
            var values = InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(raw.Metadata!)
                         ?? new Dictionary<string, object?>();
            job.Metadata = new Metadata(values);
        }
        else
        {
            job.Metadata = new Metadata();
        }

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
            ScheduleSourceType = job.ScheduleSourceType,
            Status = job.Status,
            Id = job.Id,
            OriginalScheduledAt = job.OriginalScheduledAt,
            ScheduledAt = job.ScheduledAt,
            Priority = job.Priority,
            AgentWorkerId = job.AgentWorkerId,
            MaxNumberOfRetries = job.MaxNumberOfRetries,
            Timeout = job.Timeout,
            NumberOfFailures = job.NumberOfFailures,
            MsgData = InternalJobMasterSerializer.Serialize(job.MsgData.ToDictionary()),
            Metadata = job.Metadata != null ? InternalJobMasterSerializer.Serialize(job.Metadata?.ToDictionary()) : "{}",
            CreatedAt = job.CreatedAt,
            RecurringScheduleId = job.RecurringScheduleId,
            WorkerLane = job.WorkerLane,
            PartitionLockId = job.PartitionLockId,
            PartitionLockExpiresAt = job.PartitionLockExpiresAt,
            ProcessDeadline = job.ProcessDeadline,
            ProcessingStartedAt = job.ProcessingStartedAt,
            SucceedExecutedAt = job.SucceedExecutedAt,
            Version = job.Version,
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
            ScheduleSourceType = job.ScheduleSourceType,
            Priority = job.Priority,
            Timeout = job.Timeout,
            MaxNumberOfRetries = job.MaxNumberOfRetries,
            ScheduledAt = job.OriginalScheduledAt,
            CreatedAt = job.CreatedAt,
            RecurringScheduleId = job.RecurringScheduleId,
            Metadata = job.Metadata?.ToReadable() ?? Metadata.Empty,
            MsgData = job.MsgData.ToReadable(),
            WorkerLane = job.WorkerLane,
        };
    }

    // Persistence helpers (centralized here)
    public static JobRawModel FromPersistence(JobPersistenceRecord d)
    {
        // Normalize to UTC where appropriate to avoid Kind=Unspecified surprises
        static DateTime Utc(DateTime dt) => DateTime.SpecifyKind(dt, DateTimeKind.Utc);
        static DateTime? UtcN(DateTime? dt) => dt.HasValue ? DateTime.SpecifyKind(dt.Value, DateTimeKind.Utc) : (DateTime?)null;

        var m = new JobRawModel(d.ClusterId)
        {
            Id = d.Id,
            JobDefinitionId = d.JobDefinitionId,
            ScheduleSourceType = (JobSchedulingSourceType)d.ScheduledType,
            BucketId = d.BucketId,
            AgentConnectionId = d.AgentConnectionId != null ? new AgentConnectionId(d.AgentConnectionId) : null,
            AgentWorkerId = d.AgentWorkerId,
            Priority = (JobMasterPriority)d.Priority,
            OriginalScheduledAt = Utc(d.OriginalScheduledAt),
            ScheduledAt = Utc(d.ScheduledAt),
            MsgData = string.IsNullOrEmpty(d.MsgData) ? "{}" : d.MsgData,
            Metadata = d.Metadata is null ? null : InternalJobMasterSerializer.Serialize(d.Metadata?.ToReadable().ToDictionary()),
            Status = (JobMasterJobStatus)d.Status,
            NumberOfFailures = d.NumberOfFailures,
            Timeout = TimeSpan.FromTicks(d.TimeoutTicks),
            MaxNumberOfRetries = d.MaxNumberOfRetries,
            CreatedAt = Utc(d.CreatedAt),
            RecurringScheduleId = d.RecurringScheduleId,
            PartitionLockId = d.PartitionLockId,
            PartitionLockExpiresAt = UtcN(d.PartitionLockExpiresAt),
            ProcessDeadline = UtcN(d.ProcessDeadline),
            ProcessingStartedAt = UtcN(d.ProcessingStartedAt),
            SucceedExecutedAt = UtcN(d.SucceedExecutedAt),
            WorkerLane = d.WorkerLane,
            Version = d.Version,
        };

        return m;
    }

    public static JobPersistenceRecord ToPersistence(JobRawModel m)
    {
        IWritableMetadata metadata = !string.IsNullOrEmpty(m.Metadata)? 
            new Metadata(InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(m.Metadata!)) : new Metadata();
        GenericRecordEntry metadataEntry = GenericRecordEntry.FromWritableMetadata(m.ClusterId, MasterGenericRecordGroupIds.JobMetadata, m.Id.ToString("N"), metadata);
        
        return new JobPersistenceRecord
        {
            ClusterId = m.ClusterId,
            Id = m.Id,
            JobDefinitionId = m.JobDefinitionId,
            ScheduledType = (int)m.ScheduleSourceType,
            BucketId = m.BucketId,
            AgentConnectionId = m.AgentConnectionId?.IdValue,
            AgentWorkerId = m.AgentWorkerId,
            Priority = (int)m.Priority,
            OriginalScheduledAt = m.OriginalScheduledAt,
            ScheduledAt = m.ScheduledAt,
            MsgData = string.IsNullOrEmpty(m.MsgData) ? "{}" : m.MsgData,
            Metadata = metadataEntry,
            Status = (int)m.Status,
            NumberOfFailures = m.NumberOfFailures,
            TimeoutTicks = m.Timeout.Ticks,
            MaxNumberOfRetries = m.MaxNumberOfRetries,
            CreatedAt = m.CreatedAt,
            RecurringScheduleId = m.RecurringScheduleId,
            PartitionLockId = m.PartitionLockId,
            PartitionLockExpiresAt = m.PartitionLockExpiresAt,
            ProcessDeadline = m.ProcessDeadline,
            ProcessingStartedAt = m.ProcessingStartedAt,
            SucceedExecutedAt = m.SucceedExecutedAt,
            WorkerLane = m.WorkerLane,
            Version = m.Version
        };
    }
}
