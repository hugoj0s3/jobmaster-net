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
public static class NCrontabExprExtensions
{
    public static async Task<RecurringScheduleContext> RecurringAsync<T>(
        this IJobMasterScheduler scheduler,
        string ncrontabExpression,
        IWriteableMessageData? data = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null) where T : IJobMasterHandler
    {
        var compiledExpr = RecurrenceExprCompiler.Compile(NCrontabExprCompiler.TypeId, ncrontabExpression);
        return await scheduler.RecurringAsync<T>(compiledExpr, data, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, clusterId);
    }

    public static RecurringScheduleContext Recurring<T>(
        this IJobMasterScheduler scheduler,
        string ncrontabExpression,
        IWriteableMessageData? data = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null) where T : IJobMasterHandler
    {
        var compiledExpr = RecurrenceExprCompiler.Compile(NCrontabExprCompiler.TypeId, ncrontabExpression);
        return scheduler.Recurring<T>(compiledExpr, data, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, clusterId);
    }

    public static RecurringScheduleDefinitionCollection Add<Th>(
        this RecurringScheduleDefinitionCollection collection,
        string ncrontabExpression,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        IWritableMetadata? metadata = null) where Th : class, IJobMasterHandler
    {
        var compiledExpr = RecurrenceExprCompiler.Compile(NCrontabExprCompiler.TypeId, ncrontabExpression);
        collection.Add<Th>(compiledExpr, defId, priority, timeout, maxNumberOfRetries, startAfter, endBefore, metadata);
        return collection;
    }
}
