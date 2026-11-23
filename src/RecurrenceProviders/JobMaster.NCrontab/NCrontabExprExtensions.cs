using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Abstractions.StaticRecurringSchedules;

namespace JobMaster.NCrontab;

/// <summary>
/// Convenience extensions for scheduling recurring jobs with a raw NCrontab cron expression string, without
/// needing the <c>NCrontabExprCompiler.TypeId</c> + string overloads directly. Requires
/// <see cref="JobMasterNCrontabServiceCollectionExtensions.AddJobMasterNCrontab"/> to have been called first.
/// </summary>
/// <remarks>
/// Methods taking a raw expression string are prefixed with their provider name, since a plain string doesn't say
/// which syntax it is.
/// </remarks>
public static class NCrontabExprExtensions
{
    private const string ObsoleteMessage =
        "Use the NCrontab-prefixed method instead (NCrontabRecurringAsync, NCrontabRecurring, NCrontabAdd). " +
        "Methods taking a raw expression string are prefixed with their provider name.";

    /// <summary>
    /// Creates or updates a recurring schedule that fires <typeparamref name="T"/> according to the NCrontab cron
    /// expression <paramref name="ncrontabExpression"/>. Throws if the expression can't be parsed.
    /// </summary>
    public static async Task<RecurringScheduleReceipt> NCrontabRecurringAsync<T>(
        this IJobMasterScheduler scheduler,
        string ncrontabExpression,
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
        var compiledExpr = RecurrenceExprCompiler.Compile(NCrontabExprCompiler.TypeId, ncrontabExpression);
        return await scheduler.RecurringAsync<T>(compiledExpr, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
    }

    /// <summary>Synchronous version of <see cref="NCrontabRecurringAsync{T}"/>.</summary>
    public static RecurringScheduleReceipt NCrontabRecurring<T>(
        this IJobMasterScheduler scheduler,
        string ncrontabExpression,
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
        var compiledExpr = RecurrenceExprCompiler.Compile(NCrontabExprCompiler.TypeId, ncrontabExpression);
        return scheduler.Recurring<T>(compiledExpr, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
    }

    /// <summary>Adds an NCrontab-based recurring schedule definition to <paramref name="collection"/>.</summary>
    public static RecurringScheduleDefinitionCollection NCrontabAdd<Th>(
        this RecurringScheduleDefinitionCollection collection,
        string ncrontabExpression,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where Th : class, IJobMasterHandler
    {
        collection.Add<Th>(NCrontabExprCompiler.TypeId, ncrontabExpression, defId, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
        return collection;
    }

    /// <summary>Obsolete: use <see cref="NCrontabRecurringAsync{T}"/>.</summary>
    [Obsolete(ObsoleteMessage)]
    public static Task<RecurringScheduleReceipt> RecurringAsync<T>(
        this IJobMasterScheduler scheduler,
        string ncrontabExpression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where T : IJobMasterHandler
        => scheduler.NCrontabRecurringAsync<T>(ncrontabExpression, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);

    /// <summary>Obsolete: use <see cref="NCrontabRecurring{T}"/>.</summary>
    [Obsolete(ObsoleteMessage)]
    public static RecurringScheduleReceipt Recurring<T>(
        this IJobMasterScheduler scheduler,
        string ncrontabExpression,
        IWriteableMessageData? data = null,
        string? clusterId = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where T : IJobMasterHandler
        => scheduler.NCrontabRecurring<T>(ncrontabExpression, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);

    /// <summary>Obsolete: use <see cref="NCrontabAdd{Th}"/>.</summary>
    [Obsolete(ObsoleteMessage)]
    public static RecurringScheduleDefinitionCollection Add<Th>(
        this RecurringScheduleDefinitionCollection collection,
        string ncrontabExpression,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where Th : class, IJobMasterHandler
        => collection.NCrontabAdd<Th>(ncrontabExpression, defId, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
}
