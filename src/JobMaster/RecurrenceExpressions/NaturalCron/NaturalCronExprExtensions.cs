using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Abstractions.StaticRecurringSchedules;
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
    public static async Task<RecurringScheduleReceipt> RecurringAsync<T>(
        this IJobMasterScheduler scheduler,
        NaturalCronExpr naturalCronExpr,
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
        var compiledExpr = new NaturalCronCompiledExpr(naturalCronExpr.Expression, naturalCronExpr);
        
        return await scheduler.RecurringAsync<T>(compiledExpr, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
    }
    
    /// <summary>Synchronous version of <see cref="RecurringAsync{T}"/>.</summary>
    public static RecurringScheduleReceipt Recurring<T>(
        this IJobMasterScheduler scheduler,
        NaturalCronExpr naturalCronExpr,
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
        var compiledExpr = new NaturalCronCompiledExpr(naturalCronExpr.Expression, naturalCronExpr);
        
        return scheduler.Recurring<T>(compiledExpr, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
    }
    
    /// <summary>Adds a NaturalCron-based recurring schedule definition to <paramref name="collection"/>.</summary>
    public static RecurringScheduleDefinitionCollection Add<Th>(
        this RecurringScheduleDefinitionCollection collection,
        NaturalCronExpr naturalCronExpr,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where Th : class, IJobMasterHandler
    {
        collection.Add<Th>(NaturalCronExprCompiler.TypeId, naturalCronExpr.Expression, defId, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
        return collection;
    }

    /// <summary>
    /// Creates or updates a recurring schedule that fires <typeparamref name="T"/> according to the raw NaturalCron
    /// expression <paramref name="naturalCronExpression"/>. Throws if the expression can't be parsed.
    /// </summary>
    public static async Task<RecurringScheduleReceipt> NaturalCronRecurringAsync<T>(
        this IJobMasterScheduler scheduler,
        string naturalCronExpression,
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
        var compiledExpr = RecurrenceExprCompiler.Compile(NaturalCronExprCompiler.TypeId, naturalCronExpression);
        return await scheduler.RecurringAsync<T>(compiledExpr, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
    }

    /// <summary>Synchronous version of <see cref="NaturalCronRecurringAsync{T}"/>.</summary>
    public static RecurringScheduleReceipt NaturalCronRecurring<T>(
        this IJobMasterScheduler scheduler,
        string naturalCronExpression,
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
        var compiledExpr = RecurrenceExprCompiler.Compile(NaturalCronExprCompiler.TypeId, naturalCronExpression);
        return scheduler.Recurring<T>(compiledExpr, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
    }

    /// <summary>Adds a recurring schedule definition from a raw NaturalCron expression to <paramref name="collection"/>.</summary>
    public static RecurringScheduleDefinitionCollection NaturalCronAdd<Th>(
        this RecurringScheduleDefinitionCollection collection,
        string naturalCronExpression,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where Th : class, IJobMasterHandler
    {
        collection.Add<Th>(NaturalCronExprCompiler.TypeId, naturalCronExpression, defId, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
        return collection;
    }
}