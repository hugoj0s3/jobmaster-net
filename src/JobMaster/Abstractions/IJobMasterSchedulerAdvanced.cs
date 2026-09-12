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
    /// <param name="priority">Execution priority. Falls back to <c>TDefinition</c>'s config, then <see cref="JobMasterPriority.Medium"/>.</param>
    /// <param name="workerLane">Routes the job to a dedicated worker lane. Falls back to <c>TDefinition</c>'s config, then null (default lane).</param>
    /// <param name="timeout">Maximum execution time. Falls back to <c>TDefinition</c>'s config, then the cluster default.</param>
    /// <param name="maxNumberOfRetries">Max retries on failure. Falls back to <c>TDefinition</c>'s config, then the cluster default.</param>
    /// <param name="metadata">Optional key-value metadata passed to the handler.</param>
    /// <param name="clusterId">Target cluster ID. When null, the default cluster is used.</param>
    JobContext OnceNow<TDefinition>(
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Schedules a job for <paramref name="config"/> to run immediately.</summary>
    /// <param name="config">The job definition's identity and scheduling configuration.</param>
    /// <param name="msgData">Optional payload passed to the handler.</param>
    /// <param name="clusterId">Target cluster ID. When null, the default cluster is used.</param>
    JobContext OnceNow(
        JobDefinitionConfig config,
        IWriteableMessageData? msgData = null,
        string? clusterId = null);

    /// <summary>Async version of <see cref="OnceNow{TDefinition}"/>.</summary>
    Task<JobContext> OnceNowAsync<TDefinition>(
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Async version of <see cref="OnceNow(JobDefinitionConfig, IWriteableMessageData, string)"/>.</summary>
    Task<JobContext> OnceNowAsync(
        JobDefinitionConfig config,
        IWriteableMessageData? msgData = null,
        string? clusterId = null);

    /// <summary>
    /// Schedules a job for the definition carried by <typeparamref name="TDefinition"/> to run at the specified UTC date and time.
    /// </summary>
    /// <param name="dateTime">UTC date and time when the job should execute.</param>
    /// <inheritdoc cref="OnceNow{TDefinition}" select="param[@name!='dateTime']"/>
    JobContext OnceAt<TDefinition>(
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Schedules a job for <paramref name="config"/> to run at the specified UTC date and time.</summary>
    /// <param name="dateTime">UTC date and time when the job should execute.</param>
    /// <inheritdoc cref="OnceNow(JobDefinitionConfig, IWriteableMessageData, string)" select="param[@name!='dateTime']"/>
    JobContext OnceAt(
        JobDefinitionConfig config,
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        string? clusterId = null);

    /// <summary>Async version of <see cref="OnceAt{TDefinition}"/>.</summary>
    Task<JobContext> OnceAtAsync<TDefinition>(
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Async version of <see cref="OnceAt(JobDefinitionConfig, DateTime, IWriteableMessageData, string)"/>.</summary>
    Task<JobContext> OnceAtAsync(
        JobDefinitionConfig config,
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        string? clusterId = null);

    /// <summary>
    /// Schedules a job for the definition carried by <typeparamref name="TDefinition"/> to run after the specified delay from now.
    /// </summary>
    /// <param name="after">How long to wait before executing the job.</param>
    /// <inheritdoc cref="OnceNow{TDefinition}" select="param[@name!='after']"/>
    JobContext OnceAfter<TDefinition>(
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Schedules a job for <paramref name="config"/> to run after the specified delay from now.</summary>
    /// <param name="after">How long to wait before executing the job.</param>
    /// <inheritdoc cref="OnceNow(JobDefinitionConfig, IWriteableMessageData, string)" select="param[@name!='after']"/>
    JobContext OnceAfter(
        JobDefinitionConfig config,
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        string? clusterId = null);

    /// <summary>Async version of <see cref="OnceAfter{TDefinition}"/>.</summary>
    Task<JobContext> OnceAfterAsync<TDefinition>(
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Async version of <see cref="OnceAfter(JobDefinitionConfig, TimeSpan, IWriteableMessageData, string)"/>.</summary>
    Task<JobContext> OnceAfterAsync(
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
    /// <param name="priority">Execution priority for each fired job. Falls back to <c>TDefinition</c>'s config, then <see cref="JobMasterPriority.Medium"/>.</param>
    /// <param name="workerLane">Routes fired jobs to a dedicated worker lane. Falls back to <c>TDefinition</c>'s config, then null (default lane).</param>
    /// <param name="timeout">Maximum execution time per fired job. Falls back to <c>TDefinition</c>'s config, then the cluster default.</param>
    /// <param name="maxNumberOfRetries">Max retries per fired job. Falls back to <c>TDefinition</c>'s config, then the cluster default.</param>
    /// <param name="metadata">Optional key-value metadata passed to the handler on each firing.</param>
    /// <param name="startAfter">UTC date before which no jobs fire. <c>null</c> means start immediately.</param>
    /// <param name="endBefore">UTC date after which no jobs fire. <c>null</c> means no end date.</param>
    /// <param name="clusterId">Target cluster ID. When null, the default cluster is used.</param>
    RecurringScheduleContext Recurring<TDefinition>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>
    /// Creates or updates a recurring schedule for <paramref name="config"/>, firing according to
    /// <paramref name="expression"/>. If a schedule for this definition already exists on the cluster it
    /// is updated in place.
    /// </summary>
    /// <param name="config">The job definition's identity and scheduling configuration.</param>
    /// <param name="expression">Compiled recurrence expression controlling the firing cadence.</param>
    /// <param name="data">Optional payload passed to the handler on each firing.</param>
    /// <param name="startAfter">UTC date before which no jobs fire. <c>null</c> means start immediately.</param>
    /// <param name="endBefore">UTC date after which no jobs fire. <c>null</c> means no end date.</param>
    /// <param name="clusterId">Target cluster ID. When null, the default cluster is used.</param>
    RecurringScheduleContext Recurring(
        JobDefinitionConfig config,
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null);

    /// <summary>Async version of <see cref="Recurring{TDefinition}(IRecurrenceCompiledExpr, IWriteableMessageData, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?, string)"/>.</summary>
    Task<RecurringScheduleContext> RecurringAsync<TDefinition>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Async version of <see cref="Recurring(JobDefinitionConfig, IRecurrenceCompiledExpr, IWriteableMessageData, DateTime?, DateTime?, string)"/>.</summary>
    Task<RecurringScheduleContext> RecurringAsync(
        JobDefinitionConfig config,
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null);

    /// <summary>
    /// Creates or updates a recurring schedule for the definition carried by <typeparamref name="TDefinition"/>,
    /// using a raw expression type ID and expression string. Use this overload when the expression type is
    /// resolved dynamically at runtime. If a schedule for this definition already exists on the cluster it is
    /// updated in place.
    /// </summary>
    /// <param name="expressionTypeId">The ID of the recurrence expression compiler to use.</param>
    /// <param name="expression">The raw recurrence expression string interpreted by the compiler.</param>
    /// <inheritdoc cref="Recurring{TDefinition}(IRecurrenceCompiledExpr, IWriteableMessageData, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?, string)" select="param[@name!='expressionTypeId' and @name!='expression']"/>
    RecurringScheduleContext Recurring<TDefinition>(
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
        string? clusterId = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>
    /// Creates or updates a recurring schedule for <paramref name="config"/>, using a raw expression type ID
    /// and expression string. Use this overload when the expression type is resolved dynamically at runtime.
    /// If a schedule for this definition already exists on the cluster it is updated in place.
    /// </summary>
    /// <param name="expressionTypeId">The ID of the recurrence expression compiler to use.</param>
    /// <param name="expression">The raw recurrence expression string interpreted by the compiler.</param>
    /// <inheritdoc cref="Recurring(JobDefinitionConfig, IRecurrenceCompiledExpr, IWriteableMessageData, DateTime?, DateTime?, string)" select="param[@name!='expression']"/>
    RecurringScheduleContext Recurring(
        JobDefinitionConfig config,
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null);

    /// <summary>Async version of <see cref="Recurring{TDefinition}(string, string, IWriteableMessageData, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?, string)"/>.</summary>
    Task<RecurringScheduleContext> RecurringAsync<TDefinition>(
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
        string? clusterId = null) where TDefinition : IStaticJobDefinitionConfig;

    /// <summary>Async version of <see cref="Recurring(JobDefinitionConfig, string, string, IWriteableMessageData, DateTime?, DateTime?, string)"/>.</summary>
    Task<RecurringScheduleContext> RecurringAsync(
        JobDefinitionConfig config,
        string expressionTypeId,
        string expression,
        IWriteableMessageData? data = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null);
}
