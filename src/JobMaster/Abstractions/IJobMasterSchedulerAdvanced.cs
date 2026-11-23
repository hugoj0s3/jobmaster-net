using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;

namespace JobMaster.Abstractions;

/// <summary>
/// Advanced scheduling surface for true publisher/consumer separation: schedules a job against a
/// <see cref="JobDefinitionConfig"/> (or a <see cref="JobDefinitionConfigAttribute"/> that carries one),
/// rather than against a concrete <see cref="IJobMasterHandler"/> type. A publisher can reference just
/// the definition — typically from a shared contracts assembly — without ever referencing the handler
/// that will eventually process the job. Access via <see cref="IJobMasterScheduler.Advanced"/>.
/// <para>
/// The <typeparamref name="TDefinition"/> overloads accept per-call overrides (<c>priority</c>,
/// <c>workerLane</c>, <c>timeout</c>, <c>maxNumberOfRetries</c>, <c>metadata</c>) that take precedence
/// over <typeparamref name="TDefinition"/>'s fixed <see cref="JobDefinitionConfig"/>, since that config is
/// shared by every call site using that attribute type. The <see cref="JobDefinitionConfig"/> overloads
/// have no such parameters — the config object is already yours to build, so set the values on it directly.
/// </para>
/// </summary>
public interface IJobMasterSchedulerAdvanced
{
    /// <summary>
    /// Schedules a job for the definition carried by <typeparamref name="TDefinition"/> to run immediately.
    /// </summary>
    /// <typeparam name="TDefinition">
    /// A type implementing <see cref="IStaticJobDefinitionConfig"/> that identifies the job definition —
    /// typically a <see cref="JobDefinitionConfigAttribute"/> subclass, so the same type can also be
    /// applied to the consumer's handler.
    /// </typeparam>
    /// <param name="msgData">Optional payload passed to the handler.</param>
    /// <param name="clusterId">Target cluster ID. Falls back to <c>TDefinition</c>'s config, then the default cluster.</param>
    /// <param name="priority">Execution priority. Falls back to <c>TDefinition</c>'s config, then <see cref="JobMasterPriority.Medium"/>.</param>
    /// <param name="workerLane">Routes the job to a dedicated worker lane. Falls back to <c>TDefinition</c>'s config, then null (default lane).</param>
    /// <param name="timeout">Maximum execution time. Falls back to <c>TDefinition</c>'s config, then the cluster default.</param>
    /// <param name="maxNumberOfRetries">Max retries on failure. Falls back to <c>TDefinition</c>'s config, then the cluster default.</param>
    /// <param name="metadata">Optional key-value metadata passed to the handler.</param>
    JobScheduleReceipt OnceNow<TDefinition>(
        IWriteableMessageData? msgData = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Schedules a job for <paramref name="config"/> to run immediately.</summary>
    /// <param name="config">The job definition's identity and scheduling configuration.</param>
    /// <param name="msgData">Optional payload passed to the handler.</param>
    /// <param name="clusterId">Target cluster ID. Falls back to <paramref name="config"/>'s <see cref="JobDefinitionConfig.ClusterId"/>, then the default cluster.</param>
    JobScheduleReceipt OnceNow(
        JobDefinitionConfig config,
        IWriteableMessageData? msgData = null,
        string? clusterId = null);

    /// <summary>Async version of <see cref="OnceNow{TDefinition}"/>.</summary>
    Task<JobScheduleReceipt> OnceNowAsync<TDefinition>(
        IWriteableMessageData? msgData = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Async version of <see cref="OnceNow(JobDefinitionConfig, IWriteableMessageData, string)"/>.</summary>
    Task<JobScheduleReceipt> OnceNowAsync(
        JobDefinitionConfig config,
        IWriteableMessageData? msgData = null,
        string? clusterId = null);

    /// <summary>
    /// Schedules a job for the definition carried by <typeparamref name="TDefinition"/> to run at the specified UTC date and time.
    /// </summary>
    /// <param name="dateTime">UTC date and time when the job should execute.</param>
    /// <inheritdoc cref="OnceNow{TDefinition}" select="param[@name!='dateTime']"/>
    JobScheduleReceipt OnceAt<TDefinition>(
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Schedules a job for <paramref name="config"/> to run at the specified UTC date and time.</summary>
    /// <param name="dateTime">UTC date and time when the job should execute.</param>
    /// <inheritdoc cref="OnceNow(JobDefinitionConfig, IWriteableMessageData, string)" select="param[@name!='dateTime']"/>
    JobScheduleReceipt OnceAt(
        JobDefinitionConfig config,
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        string? clusterId = null);

    /// <summary>Async version of <see cref="OnceAt{TDefinition}"/>.</summary>
    Task<JobScheduleReceipt> OnceAtAsync<TDefinition>(
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Async version of <see cref="OnceAt(JobDefinitionConfig, DateTime, IWriteableMessageData, string)"/>.</summary>
    Task<JobScheduleReceipt> OnceAtAsync(
        JobDefinitionConfig config,
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        string? clusterId = null);

    /// <summary>
    /// Schedules a job for the definition carried by <typeparamref name="TDefinition"/> to run after the specified delay from now.
    /// </summary>
    /// <param name="after">How long to wait before executing the job.</param>
    /// <inheritdoc cref="OnceNow{TDefinition}" select="param[@name!='after']"/>
    JobScheduleReceipt OnceAfter<TDefinition>(
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Schedules a job for <paramref name="config"/> to run after the specified delay from now.</summary>
    /// <param name="after">How long to wait before executing the job.</param>
    /// <inheritdoc cref="OnceNow(JobDefinitionConfig, IWriteableMessageData, string)" select="param[@name!='after']"/>
    JobScheduleReceipt OnceAfter(
        JobDefinitionConfig config,
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        string? clusterId = null);

    /// <summary>Async version of <see cref="OnceAfter{TDefinition}"/>.</summary>
    Task<JobScheduleReceipt> OnceAfterAsync<TDefinition>(
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Async version of <see cref="OnceAfter(JobDefinitionConfig, TimeSpan, IWriteableMessageData, string)"/>.</summary>
    Task<JobScheduleReceipt> OnceAfterAsync(
        JobDefinitionConfig config,
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        string? clusterId = null);

    /// <summary>
    /// Creates or updates a recurring schedule for the definition carried by <typeparamref name="TDefinition"/>,
    /// firing according to <paramref name="expression"/>. If a schedule for this definition already exists on
    /// the cluster it is updated in place.
    /// </summary>
    /// <typeparam name="TDefinition">
    /// A type implementing <see cref="IStaticJobDefinitionConfig"/> that identifies the job definition —
    /// typically a <see cref="JobDefinitionConfigAttribute"/> subclass, so the same type can also be
    /// applied to the consumer's handler.
    /// </typeparam>
    /// <param name="expression">Compiled recurrence expression controlling the firing cadence.</param>
    /// <param name="data">Optional payload passed to the handler on each firing.</param>
    /// <param name="clusterId">Target cluster ID. Falls back to <c>TDefinition</c>'s config, then the default cluster.</param>
    /// <param name="priority">Execution priority for each fired job. Falls back to <c>TDefinition</c>'s config, then <see cref="JobMasterPriority.Medium"/>.</param>
    /// <param name="workerLane">Routes fired jobs to a dedicated worker lane. Falls back to <c>TDefinition</c>'s config, then null (default lane).</param>
    /// <param name="timeout">Maximum execution time per fired job. Falls back to <c>TDefinition</c>'s config, then the cluster default.</param>
    /// <param name="maxNumberOfRetries">Max retries per fired job. Falls back to <c>TDefinition</c>'s config, then the cluster default.</param>
    /// <param name="metadata">Optional key-value metadata passed to the handler on each firing.</param>
    /// <param name="startAfter">UTC date before which no jobs fire. <c>null</c> means start immediately.</param>
    /// <param name="endBefore">Jobs fire only before this UTC time, none at or after it. <c>null</c> means no end date.</param>
    RecurringScheduleReceipt Recurring<TDefinition>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>
    /// Creates or updates a recurring schedule for <paramref name="config"/>, firing according to
    /// <paramref name="expression"/>. If a schedule for this definition already exists on the cluster it
    /// is updated in place.
    /// </summary>
    /// <param name="config">The job definition's identity and scheduling configuration.</param>
    /// <param name="expression">Compiled recurrence expression controlling the firing cadence.</param>
    /// <param name="data">Optional payload passed to the handler on each firing.</param>
    /// <param name="clusterId">Target cluster ID. Falls back to <paramref name="config"/>'s <see cref="JobDefinitionConfig.ClusterId"/>, then the default cluster.</param>
    /// <param name="startAfter">UTC date before which no jobs fire. <c>null</c> means start immediately.</param>
    /// <param name="endBefore">Jobs fire only before this UTC time, none at or after it. <c>null</c> means no end date.</param>
    RecurringScheduleReceipt Recurring(
        JobDefinitionConfig config,
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null);

    /// <summary>Async version of <see cref="Recurring{TDefinition}(IRecurrenceCompiledExpr, IWriteableMessageData, string, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?)"/>.</summary>
    Task<RecurringScheduleReceipt> RecurringAsync<TDefinition>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Async version of <see cref="Recurring(JobDefinitionConfig, IRecurrenceCompiledExpr, IWriteableMessageData, string, DateTime?, DateTime?)"/>.</summary>
    Task<RecurringScheduleReceipt> RecurringAsync(
        JobDefinitionConfig config,
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null);

    /// <summary>
    /// Creates or updates a recurring schedule for the definition carried by <typeparamref name="TDefinition"/>,
    /// using a raw expression type ID and expression string. Use this overload when the expression type is
    /// resolved dynamically at runtime. If a schedule for this definition already exists on the cluster it is
    /// updated in place.
    /// </summary>
    /// <param name="expressionTypeId">The ID of the recurrence expression compiler to use.</param>
    /// <param name="expression">The raw recurrence expression string interpreted by the compiler.</param>
    /// <inheritdoc cref="Recurring{TDefinition}(IRecurrenceCompiledExpr, IWriteableMessageData, string, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?)" select="param[@name!='expressionTypeId' and @name!='expression']"/>
    RecurringScheduleReceipt Recurring<TDefinition>(
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
        DateTime? endBefore = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>
    /// Creates or updates a recurring schedule for <paramref name="config"/>, using a raw expression type ID
    /// and expression string. Use this overload when the expression type is resolved dynamically at runtime.
    /// If a schedule for this definition already exists on the cluster it is updated in place.
    /// </summary>
    /// <param name="expressionTypeId">The ID of the recurrence expression compiler to use.</param>
    /// <param name="expression">The raw recurrence expression string interpreted by the compiler.</param>
    /// <inheritdoc cref="Recurring(JobDefinitionConfig, IRecurrenceCompiledExpr, IWriteableMessageData, string, DateTime?, DateTime?)" select="param[@name!='expression']"/>
    RecurringScheduleReceipt Recurring(
        JobDefinitionConfig config,
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null);

    /// <summary>Async version of <see cref="Recurring{TDefinition}(string, string, IWriteableMessageData, string, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?)"/>.</summary>
    Task<RecurringScheduleReceipt> RecurringAsync<TDefinition>(
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
        DateTime? endBefore = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Async version of <see cref="Recurring(JobDefinitionConfig, string, string, IWriteableMessageData, string, DateTime?, DateTime?)"/>.</summary>
    Task<RecurringScheduleReceipt> RecurringAsync(
        JobDefinitionConfig config,
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null);

    /// <summary>
    /// Makes sure every job in <paramref name="receipts"/> is saved on master, saving it now if it isn't there yet.
    /// When it returns, all of them are on master. Receipts can come from different clusters. Jobs that were already
    /// <see cref="ScheduleReceiptStatus.StoredOnMaster"/> need no call to master.
    /// <para>
    /// Rarely needed: if your agent connections are healthy, you don't need to call it at all. A confirmed job can
    /// start later than it would otherwise, because it's picked up from master instead of going straight to its
    /// worker. Prefer one batch call over many single <see cref="JobScheduleReceipt.ConfirmOnMasterAsync"/> calls.
    /// </para>
    /// <para>
    /// If it throws, some jobs may or may not be saved; calling it again is safe and never creates a second copy.
    /// JobMaster runs jobs at least once: confirming a receipt after the cluster's data retention has removed an
    /// already-finished job saves it again, and it runs again. Be careful calling it when the data retention is short:
    /// the shorter it is, the more likely a late confirm runs a job twice.
    /// </para>
    /// </summary>
    /// <param name="receipts">Receipts returned by the scheduling calls.</param>
    Task ConfirmOnMasterAsync(IEnumerable<JobScheduleReceipt> receipts);

    /// <summary>
    /// Makes sure every schedule in <paramref name="receipts"/> is saved on master, saving it now if it isn't there
    /// yet. When it returns, all of them are on master. Receipts can come from different clusters. Schedules that
    /// were already <see cref="ScheduleReceiptStatus.StoredOnMaster"/> need no call to master.
    /// <para>
    /// Rarely needed: if your agent connections are healthy, you don't need to call it at all. A confirmed schedule's
    /// first run can be delayed by up to about 10 minutes, so avoid it for schedules that recur at short intervals
    /// (under 10 minutes). Prefer one batch call over many single
    /// <see cref="RecurringScheduleReceipt.ConfirmOnMasterAsync"/> calls.
    /// </para>
    /// <para>
    /// If it throws, some schedules may or may not be saved; calling it again is safe and never creates a second
    /// copy. Confirming a receipt after the cluster's data retention has removed a cancelled or finished schedule
    /// saves it again as active. Be careful calling it when the data retention is short: the shorter it is, the more
    /// likely a late confirm brings a removed schedule back.
    /// </para>
    /// </summary>
    /// <param name="receipts">Receipts returned by the recurring scheduling calls.</param>
    Task ConfirmOnMasterAsync(IEnumerable<RecurringScheduleReceipt> receipts);
}
