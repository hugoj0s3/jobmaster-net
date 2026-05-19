using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.Abstractions.Models;

/// <summary>
/// Read-only snapshot of a recurring schedule, returned from scheduler <c>Recurring</c> calls
/// and available on <see cref="JobContext.RecurringSchedule"/> inside a handler when the job
/// was triggered by a recurring schedule.
/// </summary>
public class RecurringScheduleContext
{
    /// <summary>Unique identifier of this recurring schedule.</summary>
    public Guid Id { get; internal set; }

    /// <summary>ID of the cluster that owns this schedule.</summary>
    public string ClusterId { get; internal set; } = string.Empty;

    /// <summary>Profile ID if this schedule belongs to a static recurring schedule profile.</summary>
    public string? ProfileId { get; internal set; }

    /// <summary>UTC timestamp when this schedule was created.</summary>
    public DateTime CreatedAt { get; internal set; }

    /// <summary>Whether this schedule is dynamic (created at runtime) or static (declared in a profile).</summary>
    public RecurringScheduleType RecurringScheduleType { get; internal set; }

    /// <summary>Static definition ID, populated when this schedule originates from a static profile.</summary>
    public string? StaticDefinitionId { get; internal set; }

    /// <summary>The compiled recurrence expression that controls the firing cadence.</summary>
    public IRecurrenceCompiledExpr RecurExpression { get; internal set; } = new NeverRecursCompiledExpr();

    /// <summary>Identifier of the handler type this schedule fires.</summary>
    public string JobDefinitionId { get; internal set; } = string.Empty;

    /// <summary>UTC date after which the schedule starts firing. <c>null</c> means no start restriction.</summary>
    public DateTime? StartAfter { get; internal set; }

    /// <summary>UTC date after which the schedule stops firing. <c>null</c> means no end restriction.</summary>
    public DateTime? EndBefore { get; internal set; }

    /// <summary>Key-value metadata attached to this schedule.</summary>
    public IReadableMetadata Metadata { get; internal set; } = null!;

    /// <summary>Worker lane this schedule's jobs are routed to, or <c>null</c> for the default lane.</summary>
    public string? WorkerLane { get; internal set; }
}
