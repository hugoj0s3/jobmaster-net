using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Abstractions.StaticRecurringSchedules;

namespace JobMaster.Cronos;

/// <summary>
/// Convenience extensions for scheduling recurring jobs with a raw Cronos cron expression string, without
/// needing the <c>CronosExprCompiler.TypeId</c> + string overloads directly. Requires
/// <see cref="JobMasterCronosServiceCollectionExtensions.AddJobMasterCronos"/> to have been called first.
/// </summary>
/// <remarks>
/// Methods taking a raw expression string are prefixed with their provider name, since a plain string doesn't say
/// which syntax it is.
/// </remarks>
public static class CronosExprExtensions
{
    private const string ObsoleteMessage =
        "Use the Cronos-prefixed method instead (CronosRecurringAsync, CronosRecurring, CronosAdd). " +
        "Methods taking a raw expression string are prefixed with their provider name.";

    /// <summary>
    /// Creates or updates a recurring schedule that fires <typeparamref name="T"/> according to the Cronos cron
    /// expression <paramref name="cronExpression"/>. Throws if the expression can't be parsed.
    /// </summary>
    public static async Task<RecurringScheduleContext> CronosRecurringAsync<T>(
        this IJobMasterScheduler scheduler,
        string cronExpression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where T : IJobMasterHandler
    {
        var compiledExpr = RecurrenceExprCompiler.Compile(CronosExprCompiler.TypeId, cronExpression);
        return await scheduler.RecurringAsync<T>(compiledExpr, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
    }

    /// <summary>Synchronous version of <see cref="CronosRecurringAsync{T}"/>.</summary>
    public static RecurringScheduleContext CronosRecurring<T>(
        this IJobMasterScheduler scheduler,
        string cronExpression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where T : IJobMasterHandler
    {
        var compiledExpr = RecurrenceExprCompiler.Compile(CronosExprCompiler.TypeId, cronExpression);
        return scheduler.Recurring<T>(compiledExpr, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
    }

    /// <summary>Adds a Cronos-based recurring schedule definition to <paramref name="collection"/>.</summary>
    public static RecurringScheduleDefinitionCollection CronosAdd<Th>(
        this RecurringScheduleDefinitionCollection collection,
        string cronExpression,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where Th : class, IJobMasterHandler
    {
        var compiledExpr = RecurrenceExprCompiler.Compile(CronosExprCompiler.TypeId, cronExpression);
        collection.Add<Th>(compiledExpr, defId, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
        return collection;
    }

    /// <summary>Obsolete: use <see cref="CronosRecurringAsync{T}"/>.</summary>
    [Obsolete(ObsoleteMessage)]
    public static Task<RecurringScheduleContext> RecurringAsync<T>(
        this IJobMasterScheduler scheduler,
        string cronExpression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where T : IJobMasterHandler
        => scheduler.CronosRecurringAsync<T>(cronExpression, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);

    /// <summary>Obsolete: use <see cref="CronosRecurring{T}"/>.</summary>
    [Obsolete(ObsoleteMessage)]
    public static RecurringScheduleContext Recurring<T>(
        this IJobMasterScheduler scheduler,
        string cronExpression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where T : IJobMasterHandler
        => scheduler.CronosRecurring<T>(cronExpression, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);

    /// <summary>Obsolete: use <see cref="CronosAdd{Th}"/>.</summary>
    [Obsolete(ObsoleteMessage)]
    public static RecurringScheduleDefinitionCollection Add<Th>(
        this RecurringScheduleDefinitionCollection collection,
        string cronExpression,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where Th : class, IJobMasterHandler
        => collection.CronosAdd<Th>(cronExpression, defId, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
}
