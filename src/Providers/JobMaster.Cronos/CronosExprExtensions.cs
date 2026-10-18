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
public static class CronosExprExtensions
{
    public static async Task<RecurringScheduleContext> RecurringAsync<T>(
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
    {
        var compiledExpr = RecurrenceExprCompiler.Compile(CronosExprCompiler.TypeId, cronExpression);
        return scheduler.Recurring<T>(compiledExpr, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
    }

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
    {
        var compiledExpr = RecurrenceExprCompiler.Compile(CronosExprCompiler.TypeId, cronExpression);
        collection.Add<Th>(compiledExpr, defId, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
        return collection;
    }
}
