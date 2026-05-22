using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.RecurrenceExpressions.TimeSpanInterval;
using NaturalCron;

namespace JobMaster.RecurrenceExpressions.NaturalCron;

/// <summary>
/// Convenience extension methods for scheduling recurring jobs using <see cref="NaturalCronExpr"/> recurrence expressions.
/// </summary>
public static class NaturalCronExprExtensions
{
    /// <summary>
    /// Creates or updates a recurring schedule that fires <typeparamref name="T"/> according to <paramref name="naturalCronExpr"/>.
    /// </summary>
    public static async Task<RecurringScheduleContext> RecurringAsync<T>(
        this IJobMasterScheduler scheduler,
        NaturalCronExpr naturalCronExpr,
        IWriteableMessageData? data = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null) where T : IJobHandler
    {
        var compiledExpr = new NaturalCronCompiledExpr(naturalCronExpr.Expression, naturalCronExpr);
        
        return await scheduler.RecurringAsync<T>(compiledExpr, data, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, clusterId);
    }
    
    /// <summary>Synchronous version of <see cref="RecurringAsync{T}"/>.</summary>
    public static RecurringScheduleContext Recurring<T>(
        this IJobMasterScheduler scheduler,
        NaturalCronExpr naturalCronExpr,
        IWriteableMessageData? data = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null) where T : IJobHandler
    {
        var compiledExpr = new NaturalCronCompiledExpr(naturalCronExpr.Expression, naturalCronExpr);
        
        return scheduler.Recurring<T>(compiledExpr, data, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, clusterId);
    }
    
    /// <summary>Adds a NaturalCron-based recurring schedule definition to <paramref name="collection"/>.</summary>
    public static RecurringScheduleDefinitionCollection Add<Th>(
        this RecurringScheduleDefinitionCollection collection,
        NaturalCronExpr naturalCronExpr,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        IWritableMetadata? metadata = null) where Th : class, IJobHandler
    {
        collection.Add<Th>(TimeSpanIntervalExprCompiler.TypeId, naturalCronExpr.Expression, defId, priority, timeout, maxNumberOfRetries, startAfter, endBefore, metadata);
        return collection;
    }
}