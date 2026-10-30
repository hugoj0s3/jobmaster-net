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
/// <para>
/// Every scheduling call returns a receipt (<see cref="JobScheduleReceipt"/> / <see cref="RecurringScheduleReceipt"/>)
/// with the scheduled <c>Context</c> and where it was stored (<see cref="ScheduleReceiptStatus"/>).
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
    /// <param name="clusterId">Target cluster ID. Falls back to <c>[JobMasterClusterId]</c> attribute, then the default cluster.</param>
    /// <param name="priority">Execution priority. Falls back to <c>[JobMasterPriority]</c> attribute, then <see cref="JobMasterPriority.Medium"/>.</param>
    /// <param name="workerLane">Routes the job to a dedicated worker lane. Falls back to <c>[JobMasterWorkerLane]</c> attribute, then null (default lane).</param>
    /// <param name="timeout">Maximum execution time. Falls back to <c>[JobMasterTimeout]</c> attribute, then the cluster default.</param>
    /// <param name="maxNumberOfRetries">Max retries on failure. Falls back to <c>[JobMasterMaxNumberOfRetries]</c> attribute, then the cluster default.</param>
    /// <param name="metadata">Optional key-value metadata passed to the handler.</param>
    JobScheduleReceipt OnceNow<T>(
        IWriteableMessageData? msgData = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null) where T : IJobMasterHandler;

    /// <summary>
    /// Schedules <typeparamref name="T"/> to run at the specified UTC date and time.
    /// </summary>
    /// <param name="dateTime">UTC date and time when the job should execute.</param>
    /// <inheritdoc cref="OnceNow{T}" select="param[@name!='dateTime']"/>
    JobScheduleReceipt OnceAt<T>(
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null) where T : IJobMasterHandler;

    /// <summary>
    /// Schedules <typeparamref name="T"/> to run after the specified delay from now.
    /// </summary>
    /// <param name="after">How long to wait before executing the job.</param>
    /// <inheritdoc cref="OnceNow{T}" select="param[@name!='after']"/>
    JobScheduleReceipt OnceAfter<T>(
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null) where T : IJobMasterHandler;

    /// <summary>Async version of <see cref="OnceNow{T}"/>.</summary>
    Task<JobScheduleReceipt> OnceNowAsync<T>(
        IWriteableMessageData? msgData = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null) where T : IJobMasterHandler;

    /// <summary>Async version of <see cref="OnceAt{T}"/>.</summary>
    Task<JobScheduleReceipt> OnceAtAsync<T>(
        DateTime scheduledAt,
        IWriteableMessageData? msgData = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null) where T : IJobMasterHandler;

    /// <summary>Async version of <see cref="OnceAfter{T}"/>.</summary>
    Task<JobScheduleReceipt> OnceAfterAsync<T>(
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null) where T : IJobMasterHandler;

    /// <summary>
    /// Creates or updates a recurring schedule that fires <typeparamref name="T"/> according to <paramref name="expression"/>.
    /// If a schedule for this handler already exists on the cluster it is updated in place.
    /// </summary>
    /// <param name="expression">Compiled recurrence expression controlling the firing cadence.</param>
    /// <param name="data">Optional payload passed to the handler on each firing.</param>
    /// <param name="clusterId">Target cluster ID. Falls back to <c>[JobMasterClusterId]</c> attribute, then the default cluster.</param>
    /// <param name="priority">Execution priority for each fired job. Falls back to <c>[JobMasterPriority]</c> attribute, then <see cref="JobMasterPriority.Medium"/>.</param>
    /// <param name="workerLane">Routes fired jobs to a dedicated worker lane. Falls back to <c>[JobMasterWorkerLane]</c> attribute, then null (default lane).</param>
    /// <param name="timeout">Maximum execution time per fired job. Falls back to <c>[JobMasterTimeout]</c> attribute, then the cluster default.</param>
    /// <param name="maxNumberOfRetries">Max retries per fired job. Falls back to <c>[JobMasterMaxNumberOfRetries]</c> attribute, then the cluster default.</param>
    /// <param name="metadata">Optional key-value metadata passed to the handler on each firing.</param>
    /// <param name="startAfter">UTC date before which no jobs fire. <c>null</c> means start immediately.</param>
    /// <param name="endBefore">UTC date after which no jobs fire. <c>null</c> means no end date.</param>
    RecurringScheduleReceipt Recurring<T>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where T : IJobMasterHandler;

    /// <summary>Async version of <see cref="Recurring{T}(IRecurrenceCompiledExpr, IWriteableMessageData, string, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?)"/>.</summary>
    Task<RecurringScheduleReceipt> RecurringAsync<T>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where T : IJobMasterHandler;

    /// <summary>
    /// Creates or updates a recurring schedule using a raw expression type ID and expression string.
    /// Use this overload when the expression type is resolved dynamically at runtime.
    /// If a schedule for this handler already exists on the cluster it is updated in place.
    /// </summary>
    /// <param name="expressionTypeId">The ID of the recurrence expression compiler to use.</param>
    /// <param name="expression">The raw recurrence expression string interpreted by the compiler.</param>
    /// <inheritdoc cref="Recurring{T}(IRecurrenceCompiledExpr, IWriteableMessageData, string, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?)" select="param[@name!='expressionTypeId' and @name!='expression']"/>
    RecurringScheduleReceipt Recurring<T>(
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where T : IJobMasterHandler;

    /// <summary>Async version of <see cref="Recurring{T}(string, string, IWriteableMessageData, string, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?)"/>.</summary>
    Task<RecurringScheduleReceipt> RecurringAsync<T>(
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where T : IJobMasterHandler;

    /// <summary>
    /// Cancels the one-time job with the given ID, once it's saved on master. Right after scheduling it may not be
    /// there yet and isn't found; to cancel from that point, use <see cref="CancelJobAsync(JobScheduleReceipt)"/>.
    /// <para>
    /// Returns <c>true</c> when the job is cancelled and won't run. Returns <c>false</c> when it isn't found on
    /// master, or can't be cancelled anymore: it's running or finished, or it's due to start in less than about a
    /// minute and a worker may already have it.
    /// </para>
    /// </summary>
    /// <param name="jobId">ID of the job to cancel.</param>
    /// <param name="clusterId">
    /// Cluster the job was scheduled on. When null, the default cluster is used — the handler's
    /// <c>[JobMasterClusterId]</c> / <c>JobDefinitionConfig.ClusterId</c> is not looked up here (only the id is
    /// known), so pass it explicitly for jobs on a non-default cluster (see <see cref="JobContext.ClusterId"/>).
    /// </param>
    Task<bool> CancelJobAsync(Guid jobId, string? clusterId = null);

    /// <summary>Synchronous version of <see cref="CancelJobAsync(Guid, string)"/>.</summary>
    bool CancelJob(Guid jobId, string? clusterId = null);

    /// <summary>
    /// Cancels the job from the receipt its scheduling call returned, on the cluster it was scheduled on.
    /// <para>
    /// Unlike cancelling by id, it also works right after scheduling, before the job reaches master
    /// (<see cref="ScheduleReceiptStatus.AcceptedOnTransport"/>): the job is cancelled there and the agent's copy
    /// never runs. Once the job is on master, it behaves like <see cref="CancelJobAsync(Guid, string)"/>.
    /// </para>
    /// <para>
    /// Returns <c>true</c> when the job is cancelled and won't run. Returns <c>false</c> when it can't be cancelled
    /// anymore: it's running or finished, or it's due to start in less than about a minute and a worker may
    /// already have it.
    /// </para>
    /// <para>
    /// If it throws, the job may or may not be cancelled; calling it again is safe. Same as
    /// <see cref="JobScheduleReceipt.CancelAsync"/>.
    /// </para>
    /// </summary>
    /// <param name="receipt">Receipt returned when the job was scheduled.</param>
    Task<bool> CancelJobAsync(JobScheduleReceipt receipt);

    /// <summary>
    /// Cancels the recurring schedule with the given ID, stopping future runs, once it's saved on master. Right after
    /// scheduling it may not be there yet and isn't found; to cancel from that point, use
    /// <see cref="CancelRecurringAsync(RecurringScheduleReceipt)"/>.
    /// <para>
    /// Returns <c>true</c> when the schedule is cancelled. Returns <c>false</c> when it isn't found on master, or had
    /// already ended (cancelled, completed or inactive).
    /// </para>
    /// </summary>
    /// <param name="recurringScheduleId">ID of the recurring schedule to cancel.</param>
    /// <param name="clusterId">
    /// Cluster the schedule was created on. When null, the default cluster is used — the handler's
    /// <c>[JobMasterClusterId]</c> / <c>JobDefinitionConfig.ClusterId</c> is not looked up here (only the id is
    /// known), so pass it explicitly for schedules on a non-default cluster (see <see cref="RecurringScheduleContext.ClusterId"/>).
    /// </param>
    Task<bool> CancelRecurringAsync(Guid recurringScheduleId, string? clusterId = null);

    /// <summary>Synchronous version of <see cref="CancelRecurringAsync(Guid, string)"/>.</summary>
    bool CancelRecurring(Guid recurringScheduleId, string? clusterId = null);

    /// <summary>
    /// Cancels the recurring schedule from the receipt its scheduling call returned, on the cluster it was created
    /// on, stopping future runs.
    /// <para>
    /// Unlike cancelling by id, it also works right after scheduling, before the schedule reaches master
    /// (<see cref="ScheduleReceiptStatus.AcceptedOnTransport"/>): the schedule is cancelled there and never fires.
    /// Once the schedule is on master, it behaves like <see cref="CancelRecurringAsync(Guid, string)"/>.
    /// </para>
    /// <para>
    /// Returns <c>true</c> when the schedule is cancelled. Returns <c>false</c> when it had already ended
    /// (cancelled, completed or inactive).
    /// </para>
    /// <para>
    /// If it throws, the schedule may or may not be cancelled; calling it again is safe. Same as
    /// <see cref="RecurringScheduleReceipt.CancelAsync"/>.
    /// </para>
    /// </summary>
    /// <param name="receipt">Receipt returned when the schedule was created.</param>
    Task<bool> CancelRecurringAsync(RecurringScheduleReceipt receipt);

    /// <summary>
    /// Changes the scheduled execution time of a one-time job.
    /// The job must not have reached a final status (succeed, cancelled, aborted or failed).
    /// Returns <c>true</c> if the job was found and successfully rescheduled.
    /// </summary>
    /// <param name="jobId">ID of the job to reschedule.</param>
    /// <param name="scheduledAt">New UTC execution time.</param>
    /// <param name="clusterId">
    /// Cluster the job was scheduled on. When null, the default cluster is used — the handler's
    /// <c>[JobMasterClusterId]</c> / <c>JobDefinitionConfig.ClusterId</c> is not looked up here (only the id is
    /// known), so pass it explicitly for jobs on a non-default cluster (see <see cref="JobContext.ClusterId"/>).
    /// </param>
    Task<bool> RescheduleAsync(Guid jobId, DateTime scheduledAt, string? clusterId = null);

    /// <summary>Synchronous version of <see cref="RescheduleAsync"/>.</summary>
    bool Reschedule(Guid jobId, DateTime scheduledAt, string? clusterId = null);
}
