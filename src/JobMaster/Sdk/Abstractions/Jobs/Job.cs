using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Jobs;

internal class Job : JobMasterBaseModel
{

    internal Job(string clusterId) : base(clusterId)
    {
        Id = JobMasterRandomUtil.NewGuid7();
        Status = JobMasterJobStatus.PendingSave;
        CreatedAt = DateTime.UtcNow;
    }

    public static Job FromModel(JobRawModel rawModel)  => JobConvertUtil.ToJob(rawModel);
    
    public static JobRawModel ToModel(Job job) => JobConvertUtil.ToJobRawModel(job);
    
    public JobRawModel ToModel() => ToModel(this);
    
    public static Job New(
        string clusterId,
        Type jobHandlerType,
        IWriteableMessageData? data = null,
        DateTime? scheduledAt = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? writableMetadata = null,
        JobMasterTriggerSourceType triggerSourceType = JobMasterTriggerSourceType.Once,
        ClusterConfigurationModel? masterConfig = null,
        Guid? sourceId = null,
        string? workerLane = null)
    {
        var metadataDictionary = writableMetadata?.ToDictionary() ?? new Dictionary<string, object?>();
        var jobHandlerTypeAttributes = jobHandlerType.
            GetCustomAttributes(false)
            .OfType<JobMasterMetadataAttribute>()
            .Select(attr => attr.ToKeyValuePair())
            .ToDictionary(x => x.Key, x => x.Value);

        var finalMetadataAttribute = JobMasterDictionaryUtils.Merge(jobHandlerTypeAttributes, metadataDictionary);

        var job = NewBase(clusterId, data, scheduledAt, triggerSourceType, sourceId);
        job.JobDefinitionId = JobUtil.GetJobDefinitionId(jobHandlerType);
        job.Priority = JobUtil.GetJobMasterPriority(jobHandlerType, priority);
        job.Timeout = JobUtil.GetTimeout(jobHandlerType, timeout, masterConfig);
        job.MaxNumberOfRetries = JobUtil.GetMaxNumberOfRetries(jobHandlerType, maxNumberOfRetries, masterConfig);
        job.Metadata = new Metadata(finalMetadataAttribute);
        job.WorkerLane = JobUtil.GetWorkerLane(jobHandlerType, workerLane);

        return job;
    }
    
    public static Job FromRecurringSchedule(
        string clusterId,
        Type jobHandlerType,
        RecurringSchedule recurringSchedule, 
        DateTime scheduleAt,
        ClusterConfigurationModel? masterConfig = null)
    {
        var job = New(
            clusterId,
            jobHandlerType,
            data: recurringSchedule.MsgData,
            scheduledAt: scheduleAt,
            triggerSourceType: recurringSchedule.RecurringScheduleType == RecurringScheduleType.Static
                ? JobMasterTriggerSourceType.StaticRecurring
                : JobMasterTriggerSourceType.DynamicRecurring,
            priority: recurringSchedule.Priority,
            timeout: recurringSchedule.Timeout,
            maxNumberOfRetries: recurringSchedule.MaxNumberOfRetries,
            sourceId: recurringSchedule.Id,
            masterConfig: masterConfig,
            workerLane: recurringSchedule.WorkerLane);
        
        var recurringMetadata = recurringSchedule.Metadata?.ToDictionary() ?? new Dictionary<string, object?>();
        var finalMetadata = JobMasterDictionaryUtils.Merge(recurringMetadata, job.Metadata?.ToDictionary() ?? new Dictionary<string, object?>());
        job.Metadata = new Metadata(finalMetadata);
        
        return job;
    }

    public static Job New<T>(
        string clusterId,
        IWriteableMessageData? data = null,
        DateTime? scheduledAt = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? writableMetadata = null,
        JobMasterTriggerSourceType triggerSourceType = JobMasterTriggerSourceType.Once,
        ClusterConfigurationModel? masterConfig = null,
        string? workerLane = null)
        where T : IJobMasterHandler
    {
        return New(
            clusterId,
            typeof(T), 
            data, 
            scheduledAt, 
            priority, 
            timeout, 
            maxNumberOfRetries, 
            writableMetadata, 
            triggerSourceType, 
            masterConfig,
            workerLane: workerLane);
    }

    public static Job New(
        string clusterId,
        JobDefinitionConfig config,
        IWriteableMessageData? data = null,
        DateTime? scheduledAt = null,
        JobMasterTriggerSourceType triggerSourceType = JobMasterTriggerSourceType.Once,
        ClusterConfigurationModel? masterConfig = null,
        Guid? sourceId = null)
    {
        var job = NewBase(clusterId, data, scheduledAt, triggerSourceType, sourceId);
        job.JobDefinitionId = config.JobDefinitionId;
        job.Priority = config.Priority ?? JobMasterPriority.Medium;
        job.Timeout = config.Timeout ?? masterConfig?.DefaultJobTimeout ?? TimeSpan.FromMinutes(5);
        job.MaxNumberOfRetries = JobUtil.ValidateMaxNumberOfRetries(config.MaxNumberOfRetries ?? masterConfig?.DefaultMaxOfRetryCount ?? 3);
        job.Metadata = config.Metadata ?? new Metadata();
        job.WorkerLane = config.WorkerLane;

        return job;
    }

    public Guid Id { get; internal set; }
    public DateTime CreatedAt { get; internal set; }
    public DateTime ScheduledAt { get; internal set; }
    public  DateTime? NextPlanExecutionAt { get; internal set; }
    public  JobMasterJobStatus Status { get; internal set;}
    public  string? BucketId { get; internal set; }
    public  AgentConnectionId? AgentConnectionId { get; internal set; }
    public  HostId? HostId { get; internal set; }
    public  JobMasterPriority Priority { get; internal set;}
    public  string? AgentWorkerId { get; internal set; }
    public  string JobDefinitionId { get; internal set; } = string.Empty;
    public  JobMasterTriggerSourceType TriggerSourceType { get; internal set; }
    public  int NumberOfFailures { get; internal set; } 
    
    public Guid? PartitionLockId { get; internal set; }
    public DateTime? PartitionLockExpiresAt { get; internal set; }
    public DateTime? ProcessDeadline { get; internal set; }

    public DateTime? ProcessStartedAt { get; internal set; }

    public DateTime? FinalizedAt { get; internal set; }
    public  TimeSpan Timeout { get; internal set; }
    public  int MaxNumberOfRetries { get; internal set; }
    public IWriteableMessageData MsgData { get; internal set; } = new MessageData();
    public IWritableMetadata? Metadata { get; internal set; }
    public Guid? SourceId { get; internal set; }
    
    public string? WorkerLane { get; internal set; }

    public string? Version { get; internal set; }

    private static Job NewBase(
        string clusterId,
        IWriteableMessageData? data,
        DateTime? scheduledAt,
        JobMasterTriggerSourceType triggerSourceType,
        Guid? sourceId)
    {
        return new Job(clusterId)
        {
            TriggerSourceType = triggerSourceType,
            ScheduledAt = scheduledAt ?? DateTime.UtcNow,
            NextPlanExecutionAt = scheduledAt ?? DateTime.UtcNow,
            MsgData = data ?? MessageData.Empty,
            CreatedAt = DateTime.UtcNow,
            SourceId = sourceId
        };
    }
}