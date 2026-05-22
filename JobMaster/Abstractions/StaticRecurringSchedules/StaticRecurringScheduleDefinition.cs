using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.Abstractions.StaticRecurringSchedules;

/// <summary>
/// Describes a single static recurring schedule: its handler, recurrence expression, and execution constraints.
/// Instances are created through <see cref="RecurringScheduleDefinitionCollection"/> inside
/// <see cref="IStaticRecurringSchedulesProfile.Config"/>.
/// </summary>
public class StaticRecurringScheduleDefinition
{
    internal StaticRecurringScheduleDefinition(
        string clusterId,
        string jobDefinitionId,
        IRecurrenceCompiledExpr compiledExpr,
        string id,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        IWritableMetadata? metadata = null,
        string? workerLane = null)
    {
        ClusterId = clusterId;
        JobDefinitionId = jobDefinitionId;
        CompiledExpr = compiledExpr;
        Id = id;
        Priority = priority;
        Timeout = timeout;
        MaxNumberOfRetries = maxNumberOfRetries;
        StartAfter = startAfter;
        EndBefore = endBefore;
        Metadata = metadata;
        WorkerLane = workerLane;
    }

    /// <summary>Identifies the job handler that will be invoked on each occurrence.</summary>
    public string JobDefinitionId { get; private set; }

    /// <summary>The compiled recurrence expression that determines when the job fires.</summary>
    public IRecurrenceCompiledExpr CompiledExpr { get; private set; }

    /// <summary>The unique identifier for this schedule within its profile.</summary>
    public string Id { get; private set; }

    /// <summary>The job priority. <c>null</c> uses the handler's default priority.</summary>
    public JobMasterPriority? Priority { get; private set; }

    /// <summary>Maximum allowed execution time per occurrence. <c>null</c> uses the handler's default.</summary>
    public TimeSpan? Timeout { get; private set; }

    /// <summary>The schedule will not fire before this UTC date. <c>null</c> means no lower bound.</summary>
    public DateTime? StartAfter { get; private set; }

    /// <summary>The schedule will not fire after this UTC date. <c>null</c> means no upper bound.</summary>
    public DateTime? EndBefore { get; private set; }

    /// <summary>The cluster this schedule is scoped to.</summary>
    public string ClusterId { get; private set; }

    /// <summary>Number of retry attempts on failure. <c>null</c> uses the handler's default.</summary>
    public int? MaxNumberOfRetries { get; private set; }

    /// <summary>Optional key-value metadata attached to every job occurrence produced by this schedule.</summary>
    public IWritableMetadata? Metadata { get; private set; }

    /// <summary>
    /// The lane workers must belong to in order to execute this schedule's jobs.
    /// <c>null</c> means any worker regardless of lane.
    /// </summary>
    public string? WorkerLane { get; private set; }
}
