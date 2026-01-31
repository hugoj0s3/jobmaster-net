using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Api.ApiModels;

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
    public IDictionary<string, object?> MsgData { get; set; } = new Dictionary<string, object?>();
    public IDictionary<string, object?> Metadata { get; set; } = new Dictionary<string, object?>();
    public JobMasterJobStatus Status { get; set; }
    public int NumberOfFailures { get; set; } 
    public TimeSpan Timeout { get; set; }
    public int MaxNumberOfRetries { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public string? RecurringScheduleId { get; set; }
    public DateTime? ProcessDeadline { get; set; }
    public DateTime? ProcessingStartedAt { get; set; }
    public DateTime? SucceedExecutedAt { get; set; }
    public string? WorkerLane { get; set; }

    internal static ApiJobModel FromDomain(JobRawModel jobRawModel)
    {
        return FromDto(Job.FromModel(jobRawModel));
    }
    
    internal static ApiJobModel FromDto(Job job)
    {
        return new ApiJobModel
        {
            ClusterId = job.ClusterId,
            Id = job.Id.ToBase64(),
            JobDefinitionId = job.JobDefinitionId,
            TriggerSourceType = job.TriggerSourceType,
            BucketId = job.BucketId,
            AgentConnectionId = job.AgentConnectionId?.IdValue,
            AgentWorkerId = job.AgentWorkerId,
            Priority = job.Priority,
            OriginalScheduledAt = job.OriginalScheduledAt,
            ScheduledAt = job.ScheduledAt,
            MsgData = job.MsgData.ToDictionary(),
            Metadata = job.Metadata?.ToDictionary() ?? new Dictionary<string, object?>(),
            Status = job.Status,
            NumberOfFailures = job.NumberOfFailures,
            Timeout = job.Timeout,
            MaxNumberOfRetries = job.MaxNumberOfRetries,
            CreatedAt = job.CreatedAt,
            RecurringScheduleId = job.RecurringScheduleId.HasValue ? job.RecurringScheduleId.Value.ToBase64() : null,
            ProcessDeadline = job.ProcessDeadline,
            ProcessingStartedAt = job.ProcessingStartedAt,
            SucceedExecutedAt = job.SucceedExecutedAt,
            WorkerLane = job.WorkerLane,
        };
    }
}