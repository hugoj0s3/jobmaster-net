using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Api.ApiModels;

/// <summary>Represents a scheduled job as returned by the API.</summary>
public class ApiJobModel : ApiClusterBaseModel
{
    /// <summary>Unique identifier of the job (base64url-encoded GUID).</summary>
    public string Id { get; set; } = string.Empty;
    /// <summary>Handler definition ID that processes this job.</summary>
    public string JobDefinitionId { get; set; } = string.Empty;
    /// <summary>What triggered this job (once, static recurring, or dynamic recurring).</summary>
    public JobMasterTriggerSourceType TriggerSourceType { get; set; }
    /// <summary>Bucket that owns this job, if assigned.</summary>
    public string? BucketId { get; set; }
    /// <summary>Agent connection id that dispatched this job, if assigned.</summary>
    public string? AgentConnectionId { get; set; }
    /// <summary>
    /// Agent connection name 
    /// </summary>
    public string? AgentConnectionName { get; set; }
    /// <summary>Worker that is executing or executed this job, if assigned.</summary>
    public string? AgentWorkerId { get; set; }
    /// <summary>Host running the assigned worker, if known.</summary>
    public string? HostId { get; set; }
    /// <summary>Display name of the host running the assigned worker, if known.</summary>
    public string? HostDisplayName { get; set; } = string.Empty;
    /// <summary>Execution priority of the job.</summary>
    public JobMasterPriority Priority { get; set; }
    /// <summary>UTC timestamp when the job is scheduled to execute.</summary>
    public DateTime ScheduledAt { get; set; }
    /// <summary>UTC timestamp of the next planned execution window, if applicable.</summary>
    public DateTime? NextPlanExecutionAt { get; set; }
    /// <summary>Message payload attached to this job.</summary>
    public IDictionary<string, object?> MsgData { get; set; } = new Dictionary<string, object?>();
    /// <summary>Key-value metadata attached to this job.</summary>
    public IDictionary<string, object?> Metadata { get; set; } = new Dictionary<string, object?>();
    /// <summary>Current lifecycle status of the job.</summary>
    public JobMasterJobStatus Status { get; set; }
    /// <summary>Number of previous failed execution attempts.</summary>
    public int NumberOfFailures { get; set; }
    /// <summary>Maximum allowed execution time for this job.</summary>
    public TimeSpan Timeout { get; set; }
    /// <summary>Maximum number of automatic retries after failure.</summary>
    public int MaxNumberOfRetries { get; set; }
    /// <summary>UTC timestamp when the job was created.</summary>
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    /// <summary>Identifier of the source (e.g. recurring schedule) that produced this job.</summary>
    public string? SourceId { get; set; }
    /// <summary>Execution deadline; the job is considered timed-out after this UTC timestamp.</summary>
    public DateTime? ProcessDeadline { get; set; }
    /// <summary>UTC timestamp when execution started, if applicable.</summary>
    public DateTime? ProcessStartedAt { get; set; }
    /// <summary>UTC timestamp when the job reached a final status, if applicable.</summary>
    public DateTime? FinalizedAt { get; set; }
    /// <summary>Worker lane this job is routed to, or <c>null</c> for the default lane.</summary>
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
            AgentConnectionName = job.AgentConnectionId?.Name,
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
            FinalizedAt = job.FinalizedAt,
            WorkerLane = job.WorkerLane,
            HostId = job.HostId?.IdValue,
            HostDisplayName = job.HostId?.HostDisplayName,
        };
    }
}