using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Internals;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Jobs;
public class Job : JobMasterBaseModel
{

    internal Job(string clusterId) : base(clusterId)
    {
        Id = JobMasterRandomUtil.NewGuid();
        Status = JobMasterJobStatus.SavePending;
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
        JobSchedulingSourceType scheduledType = JobSchedulingSourceType.Once,
        ClusterConfigurationModel? masterConfig = null,
        Guid? recurringScheduleId = null,
        string? workerLane = null)
    {
        var metadataDictionary = writableMetadata?.ToDictionary() ?? new Dictionary<string, object?>();
        var jobHandlerTypeAttributes = jobHandlerType.
            GetCustomAttributes(false)
            .OfType<JobMasterMetadataAttribute>()
            .Select(attr => attr.ToKeyValuePair())
            .ToDictionary(x => x.Key, x => x.Value);

        var finalMetadataAttribute = JobMasterDictionaryUtils.Merge(jobHandlerTypeAttributes, metadataDictionary);
        
        var job = new Job(clusterId)
        {
            JobDefinitionId = JobUtil.GetJobDefinitionId(jobHandlerType),
            ScheduleSourceType = scheduledType,
            OriginalScheduledAt = scheduledAt ?? DateTime.UtcNow,
            ScheduledAt = scheduledAt ?? DateTime.UtcNow,
            Priority = JobUtil.GetJobMasterPriority(jobHandlerType, priority),
            Timeout = JobUtil.GetTimeout(jobHandlerType, timeout, masterConfig),
            MaxNumberOfRetries = JobUtil.GetMaxNumberOfRetries(jobHandlerType, maxNumberOfRetries, masterConfig),
            MsgData = data ?? MessageData.Empty,
            Metadata = new Metadata(finalMetadataAttribute),
            CreatedAt = DateTime.UtcNow,
            RecurringScheduleId = recurringScheduleId,
            WorkerLane = JobUtil.GetWorkerLane(jobHandlerType, workerLane)
        };
        
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
            scheduledType: recurringSchedule.RecurringScheduleType == RecurringScheduleType.Static
                ? JobSchedulingSourceType.StaticRecurring
                : JobSchedulingSourceType.DynamicRecurring,
            priority: recurringSchedule.Priority,
            timeout: recurringSchedule.Timeout,
            maxNumberOfRetries: recurringSchedule.MaxNumberOfRetries,
            recurringScheduleId: recurringSchedule.Id,
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
        JobSchedulingSourceType scheduledType = JobSchedulingSourceType.Once,
        ClusterConfigurationModel? masterConfig = null,
        string? workerLane = null)
        where T : IJobHandler
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
            scheduledType, 
            masterConfig,
            workerLane: workerLane);
    }
    
    public Guid Id { get; internal set; }
    public DateTime CreatedAt { get; internal set; }
    public DateTime OriginalScheduledAt { get; internal set; }
    public  DateTime ScheduledAt { get; internal set; }
    public  JobMasterJobStatus Status { get; internal set;}
    public  string? BucketId { get; internal set; }
    public  AgentConnectionId? AgentConnectionId { get; internal set; }
    public  JobMasterPriority Priority { get; internal set;}
    public  string? AgentWorkerId { get; internal set; }
    public  string JobDefinitionId { get; internal set; } = string.Empty;
    public  JobSchedulingSourceType ScheduleSourceType { get; internal set; }
    public  int NumberOfFailures { get; internal set; } 
    
    public int? PartitionLockId { get; internal set; }
    public DateTime? PartitionLockExpiresAt { get; internal set; }
    public DateTime? ProcessDeadline { get; internal set; }

    public DateTime? ProcessingStartedAt { get; internal set; }

    public DateTime? SucceedExecutedAt { get; internal set; }
    public  TimeSpan Timeout { get; internal set; }
    public  int MaxNumberOfRetries { get; internal set; }
    public IWriteableMessageData MsgData { get; internal set; } = new MessageData();
    public IWritableMetadata? Metadata { get; internal set; }
    public Guid? RecurringScheduleId { get; internal set; }
    
    public string? WorkerLane { get; internal set; }
    
    public string? Version { get; internal set; }
}