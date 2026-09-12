using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Jobs;

/// <summary>
/// Centralized conversions among Job, JobRawModel, and JobContext.
/// Prefer these helpers over scattered conversions. Persistence-record conversions live with
/// each repository family instead (e.g. JobMaster.SqlBase.Models.Jobs.SqlJobPersistenceConvertUtil).
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

}
