using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.Abstractions;

/// <summary>
/// Main entry point for scheduling one-time and recurring jobs.
/// Inject this interface to schedule jobs from application code.
/// <para>
/// Per-call parameters (<c>priority</c>, <c>workerLane</c>, <c>timeout</c>, <c>maxNumberOfRetries</c>, <c>metadata</c>)
/// take precedence over any attribute-level defaults declared on the handler class.
/// </para>
/// </summary>
public interface IJobMasterScheduler
{
    /// <summary>
    /// Advanced scheduling surface for true publisher/consumer separation — schedules against a
    /// <see cref="JobDefinitionConfig"/> instead of a concrete <see cref="IJobMasterHandler"/> type.
    /// See <see cref="IJobMasterSchedulerAdvanced"/>.
    /// </summary>
    IJobMasterSchedulerAdvanced Advanced { get; }

    /// <summary>
    /// Schedules <typeparamref name="T"/> to run immediately.
    /// </summary>
    /// <param name="msgData">Optional payload passed to the handler.</param>
    /// <param name="priority">Execution priority. Falls back to <c>[JobMasterPriority]</c> attribute, then <see cref="JobMasterPriority.Medium"/>.</param>
    /// <param name="workerLane">Routes the job to a dedicated worker lane. Falls back to <c>[JobMasterWorkerLane]</c> attribute, then null (default lane).</param>
    /// <param name="timeout">Maximum execution time. Falls back to <c>[JobMasterTimeout]</c> attribute, then the cluster default.</param>
    /// <param name="maxNumberOfRetries">Max retries on failure. Falls back to <c>[JobMasterMaxNumberOfRetries]</c> attribute, then the cluster default.</param>
    /// <param name="metadata">Optional key-value metadata passed to the handler.</param>
    /// <param name="clusterId">Target cluster ID. When null, the default cluster is used.</param>
    JobContext OnceNow<T>(
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobMasterHandler;

    /// <summary>
    /// Schedules <typeparamref name="T"/> to run at the specified UTC date and time.
    /// </summary>
    /// <param name="dateTime">UTC date and time when the job should execute.</param>
    /// <inheritdoc cref="OnceNow{T}" select="param[@name!='dateTime']"/>
    JobContext OnceAt<T>(
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobMasterHandler;

    /// <summary>
    /// Schedules <typeparamref name="T"/> to run after the specified delay from now.
    /// </summary>
    /// <param name="after">How long to wait before executing the job.</param>
    /// <inheritdoc cref="OnceNow{T}" select="param[@name!='after']"/>
    JobContext OnceAfter<T>(
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobMasterHandler;

    /// <summary>Async version of <see cref="OnceNow{T}"/>.</summary>
    Task<JobContext> OnceNowAsync<T>(
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobMasterHandler;

    /// <summary>Async version of <see cref="OnceAt{T}"/>.</summary>
    Task<JobContext> OnceAtAsync<T>(
        DateTime scheduledAt,
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobMasterHandler;

    /// <summary>Async version of <see cref="OnceAfter{T}"/>.</summary>
    Task<JobContext> OnceAfterAsync<T>(
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobMasterHandler;

    /// <summary>
    /// Creates or updates a recurring schedule that fires <typeparamref name="T"/> according to <paramref name="expression"/>.
    /// If a schedule for this handler already exists on the cluster it is updated in place.
    /// </summary>
    /// <param name="expression">Compiled recurrence expression controlling the firing cadence.</param>
    /// <param name="data">Optional payload passed to the handler on each firing.</param>
    /// <param name="priority">Execution priority for each fired job. Falls back to <c>[JobMasterPriority]</c> attribute, then <see cref="JobMasterPriority.Medium"/>.</param>
    /// <param name="workerLane">Routes fired jobs to a dedicated worker lane. Falls back to <c>[JobMasterWorkerLane]</c> attribute, then null (default lane).</param>
    /// <param name="timeout">Maximum execution time per fired job. Falls back to <c>[JobMasterTimeout]</c> attribute, then the cluster default.</param>
    /// <param name="maxNumberOfRetries">Max retries per fired job. Falls back to <c>[JobMasterMaxNumberOfRetries]</c> attribute, then the cluster default.</param>
    /// <param name="metadata">Optional key-value metadata passed to the handler on each firing.</param>
    /// <param name="startAfter">UTC date before which no jobs fire. <c>null</c> means start immediately.</param>
    /// <param name="endBefore">UTC date after which no jobs fire. <c>null</c> means no end date.</param>
    /// <param name="clusterId">Target cluster ID. When null, the default cluster is used.</param>
    RecurringScheduleContext Recurring<T>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null) where T : IJobMasterHandler;

    /// <summary>Async version of <see cref="Recurring{T}(IRecurrenceCompiledExpr, IWriteableMessageData, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?, string)"/>.</summary>
    Task<RecurringScheduleContext> RecurringAsync<T>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null) where T : IJobMasterHandler;

    /// <summary>
    /// Creates or updates a recurring schedule using a raw expression type ID and expression string.
    /// Use this overload when the expression type is resolved dynamically at runtime.
    /// If a schedule for this handler already exists on the cluster it is updated in place.
    /// </summary>
    /// <param name="expressionTypeId">The ID of the recurrence expression compiler to use.</param>
    /// <param name="expression">The raw recurrence expression string interpreted by the compiler.</param>
    /// <inheritdoc cref="Recurring{T}(IRecurrenceCompiledExpr, IWriteableMessageData, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?, string)" select="param[@name!='expressionTypeId' and @name!='expression']"/>
    RecurringScheduleContext Recurring<T>(
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null) where T : IJobMasterHandler;

    /// <summary>Async version of <see cref="Recurring{T}(string, string, IWriteableMessageData, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?, string)"/>.</summary>
    Task<RecurringScheduleContext> RecurringAsync<T>(
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null) where T : IJobMasterHandler;

    /// <summary>
    /// Cancels the one-time job with the given ID.
    /// Returns <c>true</c> if the job was found and successfully cancelled.
    /// </summary>
    /// <param name="jobId">ID of the job to cancel.</param>
    /// <param name="clusterId">Target cluster ID. When null, the default cluster is used.</param>
    Task<bool> CancelJobAsync(Guid jobId, string? clusterId = null);

    /// <summary>Synchronous version of <see cref="CancelJobAsync"/>.</summary>
    bool TryCancelJob(Guid id, string? clusterId = null);

    /// <summary>
    /// Cancels the recurring schedule with the given ID, stopping future firings.
    /// Returns <c>true</c> if the schedule was found and successfully cancelled.
    /// </summary>
    /// <param name="id">ID of the recurring schedule to cancel.</param>
    /// <param name="clusterId">Target cluster ID. When null, the default cluster is used.</param>
    Task<bool> TryCancelRecurringAsync(Guid id, string? clusterId = null);

    /// <summary>Synchronous version of <see cref="TryCancelRecurringAsync"/>.</summary>
    bool CancelRecurring(Guid id, string? clusterId = null);

    /// <summary>
    /// Changes the scheduled execution time of a one-time job.
    /// The job must not have reached a final status (succeed, cancelled, aborted or failed).
    /// Returns <c>true</c> if the job was found and successfully rescheduled.
    /// </summary>
    /// <param name="jobId">ID of the job to reschedule.</param>
    /// <param name="scheduledAt">New UTC execution time.</param>
    /// <param name="clusterId">Target cluster ID. When null, the default cluster is used.</param>
    Task<bool> ReScheduleAsync(Guid jobId, DateTime scheduledAt, string? clusterId = null);

    /// <summary>Synchronous version of <see cref="ReScheduleAsync"/>.</summary>
    bool ReSchedule(Guid jobId, DateTime scheduledAt, string? clusterId = null);
}
