using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Api.ApiModels;

public class ApiJobModel : ApiClusterBaseModel
{
    public string Id { get; set; } = string.Empty;
    public string JobDefinitionId { get; set; } = string.Empty;
    public JobMasterTriggerSourceType TriggerSourceType { get; set; }
    public string? BucketId { get; set; }
    public string? AgentConnectionId { get; set; }
    public string? AgentWorkerId { get; set; }
    public string? HostId { get; set; }
    public string? HostDisplayName { get; set; } = string.Empty;
    public JobMasterPriority Priority { get; set; }
    public DateTime ScheduledAt { get; set; }
    public DateTime NextPlanExecutionAt { get; set; }
    public IDictionary<string, object?> MsgData { get; set; } = new Dictionary<string, object?>();
    public IDictionary<string, object?> Metadata { get; set; } = new Dictionary<string, object?>();
    public JobMasterJobStatus Status { get; set; }
    public int NumberOfFailures { get; set; } 
    public TimeSpan Timeout { get; set; }
    public int MaxNumberOfRetries { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public string? SourceId { get; set; }
    public DateTime? ProcessDeadline { get; set; }
    public DateTime? ProcessStartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
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
            ScheduledAt = job.ScheduledAt,
            NextPlanExecutionAt = job.NextPlanExecutionAt,
            MsgData = job.MsgData.ToDictionary(),
            Metadata = job.Metadata?.ToDictionary() ?? new Dictionary<string, object?>(),
            Status = job.Status,
            NumberOfFailures = job.NumberOfFailures,
            Timeout = job.Timeout,
            MaxNumberOfRetries = job.MaxNumberOfRetries,
            CreatedAt = job.CreatedAt,
            SourceId = job.SourceId?.ToBase64(),
            ProcessDeadline = job.ProcessDeadline,
            ProcessStartedAt = job.ProcessStartedAt,
            CompletedAt = job.CompletedAt,
            WorkerLane = job.WorkerLane,
            HostId = job.HostId?.IdValue,
            HostDisplayName = job.HostId?.HostDisplayName,
        };
    }
}