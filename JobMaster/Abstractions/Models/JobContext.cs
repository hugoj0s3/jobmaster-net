namespace JobMaster.Abstractions.Models;

/// <summary>
/// Read-only snapshot of a scheduled job, passed to <see cref="IJobHandler.HandleAsync"/>.
/// Contains the job's payload, metadata, and scheduling information as set at the time of dispatch.
/// </summary>
public class JobContext
{
    /// <summary>Unique identifier of this job instance.</summary>
    public Guid Id { get; internal set; }

    /// <summary>ID of the cluster that owns this job.</summary>
    public string ClusterId { get; internal set; } = string.Empty;

    /// <summary>UTC timestamp when the job was created.</summary>
    public DateTime CreatedAt { get; internal set; }

    /// <summary>UTC timestamp when the job was scheduled to run.</summary>
    public DateTime ScheduledAt { get; internal set; }

    /// <summary>Execution priority of this job.</summary>
    public JobMasterPriority Priority { get; internal set;}

    /// <summary>Identifier of the handler type responsible for this job.</summary>
    public string JobDefinitionId { get; internal set; } = string.Empty;

    /// <summary>Number of previous failed execution attempts.</summary>
    public int NumberOfFailures { get; internal set; }

    /// <summary>Maximum allowed execution time for this job.</summary>
    public TimeSpan Timeout { get; internal set; }

    /// <summary>Maximum number of automatic retries after failure.</summary>
    public int MaxNumberOfRetries { get; internal set; }

    /// <summary>Message data payload attached to this job.</summary>
    public IReadableMessageData MsgData { get; internal set; } = null!;

    /// <summary>Key-value metadata attached to this job.</summary>
    public IReadableMetadata Metadata { get; internal set; } = null!;

    /// <summary>ID of the source that triggered this job, if applicable (e.g. the recurring schedule ID).</summary>
    public Guid? SourceId { get; internal set; }

    /// <summary>Indicates what triggered this job.</summary>
    public JobMasterTriggerSourceType TriggerSourceType { get; internal set; }

    /// <summary>The recurring schedule that produced this job, populated when <see cref="TriggerSourceType"/> is a recurring schedule.</summary>
    public RecurringScheduleContext? RecurringSchedule { get; internal set; }

    /// <summary>Worker lane this job is routed to, or <c>null</c> for the default lane.</summary>
    public string? WorkerLane { get; internal set; }
}
