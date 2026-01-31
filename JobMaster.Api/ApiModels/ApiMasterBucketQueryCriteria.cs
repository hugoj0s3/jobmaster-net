using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Utils;

namespace JobMaster.Api.ApiModels;

public enum ApiJobMasterLogLevel
{
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

public enum ApiJobMasterLogSubjectType
{
    Job = 1,
    JobExecution = 2,
    AgentWorker = 3,
    Bucket = 4,
    Cluster = 5,
    RecurringSchedule = 6,
    Api = 7,
}


public class ApiLogItem
{
    public string Id { get; set; } = string.Empty;
    
    public string ClusterId { get; set; } = string.Empty;
    public ApiJobMasterLogLevel Level { get; set; }
    public string Message { get; set; } = string.Empty;
    public ApiJobMasterLogSubjectType? SubjectType { get; set; }
    public string? SubjectId { get; set; }
    public DateTime TimestampUtc { get; set; }
    public string? Host { get; set; }

    public string? SourceMember { get; set; }
    public string? SourceFile { get; set; }
    public int? SourceLine { get; set; }

    // Reduce the payload size.
    // TODO ideally it should be done at repository level.
    // One Idea:
    //    - Create new field 'Summary' that will contain the first 100 characters of the message.
    //    - On Generic repository create criteria that does not include certain fields.
    public void CutMessage()
    {
        if (Message.Length > 100)
        {
            Message = Message.Substring(0, 100) + "...";
        }
    }
}

public class ApiLogItemQueryCriteria
{
    public ApiJobMasterLogLevel? Level { get; set; }
    public ApiJobMasterLogSubjectType? SubjectType { get; set; }
    public string? SubjectId { get; set; }
    
    public DateTime? FromTimestamp { get; set; }
    public DateTime? ToTimestamp { get; set; }
    
    public string? Keyword { get; set; }
    
    public int CountLimit { get; set; } = 100;
    public int Offset { get; set; }
}

public class ApiMasterBucketQueryCriteria
{
    public string? AgentConnectionId { get; set; }
    public JobMasterPriority? Priority { get; set; }
    public BucketStatus? Status { get; set; }
    public string? AgentWorkerId { get; set; }
    public string? WorkerLane { get; set; }

    internal MasterBucketQueryCriteria ToDomainCriteria()
    {
        return new MasterBucketQueryCriteria()
        {
            AgentConnectionId = AgentConnectionId,
            AgentWorkerId = AgentWorkerId,
            Priority = Priority,
            Status = Status,
            WorkerLane = WorkerLane,
        };
    }
}

public class ApiClusterBaseModel 
{
    public string ClusterId { get; set; } = string.Empty;
}

public class ApiBucketModel : ApiClusterBaseModel
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string AgentConnectionId { get; set; } = null!;
    public string? AgentWorkerId { get; set; }
    public string RepositoryTypeId { get; set; } = string.Empty;
    public JobMasterPriority Priority { get; set; }
    public BucketStatus Status { get; set; }
    public DateTime CreatedAt { get;  set; }
    public string Color { get; set; } = string.Empty;
    public string? WorkerLane { get; set; }
    public DateTime LastStatusChangeAt { get; set; }

    internal static ApiBucketModel FromDomain(BucketModel model)
    {
        return new ApiBucketModel()
        {
            AgentConnectionId = model.AgentConnectionId.IdValue,
            AgentWorkerId = model.AgentWorkerId,
            Priority = model.Priority,
            Status = model.Status,
            WorkerLane = model.WorkerLane,
            Id = model.Id,
            Name = model.Name,
            CreatedAt = model.CreatedAt,
            Color = model.Color.ToBucketColorHex(),
            LastStatusChangeAt = model.LastStatusChangeAt,
            ClusterId = model.ClusterId,
            RepositoryTypeId = model.RepositoryTypeId,
        };
    }
}

public class ApiAgentWorkerCriteria
{
    public string? AgentConnectionId { get; set; }
    public string? WorkerLane { get; set; }
    public AgentWorkerStatus? Status { get; set; }
    public AgentWorkerMode? Mode { get; set; }
    public bool? IsAlive { get; set; }
}

public class ApiAgentWorker : ApiClusterBaseModel
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string AgentConnectionId { get; set; } = null!;
    public DateTime CreatedAt { get; set; }
    public bool IsAlive { get; set; }
    public DateTime? StopRequestedAt { get; set; }
    public TimeSpan? StopGracePeriod { get; set; }
    public DateTime LastHeartbeat { get; set; }
    public AgentWorkerMode Mode { get; set; }
    public string? WorkerLane { get; set; }
    public double ParallelismFactor { get; set; }
    public AgentWorkerStatus Status { get; set; }
}

public class ApiClusterModel : ApiClusterBaseModel
{
    public string RepositoryTypeId { get; set; } = string.Empty;
    public TimeSpan DefaultJobTimeout { get; set; }
    public TimeSpan TransientThreshold { get; set; }
    public int DefaultMaxOfRetryCount { get; set; }
    public ClusterMode ClusterMode { get; set; }
    public int MaxMessageByteSize { get; set; }
    public string IanaTimeZoneId { get; set; } = string.Empty;
    public TimeSpan? DataRetentionTtl { get; set; }
    public IDictionary<string, object> AdditionalConfig { get; set; } = new Dictionary<string, object>();
}

public enum ApiGenericFilterOperation
{
    Eq,
    Neq,
    In,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    StartsWith,
    EndsWith,
}

public class ApiGenericRecordValueFilter
{
    public string Key { get; set; } = string.Empty;

    public ApiGenericFilterOperation Operation { get; set; }

    public object? Value { get; set; }

    public IReadOnlyList<object?>? Values { get; set; }
}

public class ApiJobQueryCriteria
{
    public JobMasterJobStatus? Status { get; set; }
    public DateTime? ScheduledTo { get; set; }
    public DateTime? ScheduledFrom { get; set; }
    public DateTime? ProcessDeadlineTo { get; set; }
    public string? RecurringScheduleId { get; set; }
    public IList<ApiGenericRecordValueFilter> MetadataFilters { get; set; } = new List<ApiGenericRecordValueFilter>();
    public string? JobDefinitionId { get; set; }
    public string? WorkerLane { get; set; }
    public int CountLimit { get; set; }
    public int Offset { get; set; }
}

public class ApiJobModel : ApiClusterBaseModel
{
    public string Id { get; set; } = string.Empty;
    public string JobDefinitionId { get; set; } = string.Empty;
    public JobSchedulingTriggerSourceType TriggerSourceType { get; set; }
    public string? BucketId { get; set; }
    public string? AgentConnectionId { get; set; }
    public string? AgentWorkerId { get; set; }
    public JobMasterPriority Priority { get; set; }
    public DateTime OriginalScheduledAt { get; set; }
    public DateTime ScheduledAt { get; set; }
    public IDictionary<string, object> MsgData { get; set; } = new Dictionary<string, object>();
    public IDictionary<string, object> Metadata { get; set; } = new Dictionary<string, object>();
    public JobMasterJobStatus Status { get; set; }
    public int NumberOfFailures { get; set; } 
    public TimeSpan Timeout { get; set; }
    public int MaxNumberOfRetries { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public Guid? RecurringScheduleId { get; set; }
    public DateTime? ProcessDeadline { get; set; }
    public DateTime? ProcessingStartedAt { get; set; }
    public DateTime? SucceedExecutedAt { get; set; }
    public string? WorkerLane { get; set; }
}

public class ApiRecurringScheduleQueryCriteria
{
    public RecurringScheduleStatus? Status { get; set; }
    public DateTime? StartAfterTo { get; set; }
    public DateTime? StartAfterFrom { get; set; }
    public DateTime? EndBeforeTo { get; set; }
    public DateTime? EndBeforeFrom { get; set; }
    public DateTime? CoverageUntil { get; set; }
    public bool? IsJobCancellationPending { get; set; }
    public bool? CanceledOrInactive { get; set; }
    public RecurringScheduleType? RecurringScheduleType { get; set; }
    public string? JobDefinitionId { get; set; }
    public string? ProfileId { get; set; }
    public string? WorkerLane { get; set; }
    public IList<ApiGenericRecordValueFilter> MetadataFilters { get; set; } = new List<ApiGenericRecordValueFilter>();
    
    public int CountLimit { get; set; } = 100;
    public int Offset { get; set; }
}

public class ApiRecurringScheduleModel : ApiClusterBaseModel
{
    public string Id { get; set; } = string.Empty;
    
    public string Expression { get; set; } = string.Empty;
    
    public string ExpressionTypeId { get; set; } = string.Empty;
    
    public string JobDefinitionId { get; set; } = string.Empty;
    
    public string? StaticDefinitionId { get; set; }
    
    public string? ProfileId { get; set; }
    
    public RecurringScheduleStatus Status { get; set; }
    
    public RecurringScheduleType RecurringScheduleType { get; set; }
    
    public DateTime? TerminatedAt { get; set; }
    
    public IDictionary<string, object> MsgData { get; set; } = new Dictionary<string, object>();
    
    public IDictionary<string, object> Metadata { get; set; } = new Dictionary<string, object>();
    
    public JobMasterPriority? Priority { get; set; }
    
    public int? MaxNumberOfRetries { get; set; }
    
    public TimeSpan? Timeout { get; set; }
    
    public DateTime CreatedAt { get; set; }
    
    public DateTime? StartAfter { get; set; }
    
    public DateTime? EndBefore { get; set; }
    
    public DateTime? LastPlanCoverageUntil { get; set; }
    
    public DateTime? LastExecutedPlan { get; set; }
    
    public bool? HasFailedOnLastPlanExecution { get; set; }
    
    public bool? IsJobCancellationPending { get; set; }
    
    public DateTime? StaticDefinitionLastEnsured { get; set; }
    
    public string? WorkerLane { get; set; }
    
    public bool IsStaticIdle { get; set; }
}


internal static class GuidBase64Extensions
{
    public static string ToBase64(this Guid guid)
    {
        var bytes = guid.ToByteArray(); // 16 bytes
        var b64 = Convert.ToBase64String(bytes); // 24 chars with "=="
        return b64.TrimEnd('=').Replace('+', '-').Replace('/', '_'); // 22 chars
    }

    public static Guid FromBase64(this string base64Url)
    {
        if (string.IsNullOrWhiteSpace(base64Url))
            throw new ArgumentException("Value cannot be null or empty.", nameof(base64Url));

        var b64 = base64Url.Replace('-', '+').Replace('_', '/');

        // restore padding
        switch (b64.Length % 4)
        {
            case 0: break;
            case 2: b64 += "=="; break;
            case 3: b64 += "="; break;
            default:
                throw new FormatException("Invalid base64url string length.");
        }

        var bytes = Convert.FromBase64String(b64);
        if (bytes.Length != 16)
            throw new FormatException("Invalid GUID base64url payload length.");

        return new Guid(bytes);
    }
}