using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.StaticRecurringSchedules;

namespace JobMaster.RecurrenceExpressions.TimeSpanInterval;

/// <summary>
/// Convenience extension methods for scheduling fixed-interval recurring jobs using <see cref="TimeSpan"/> values.
/// </summary>
public static class TimeSpanIntervalCompiledExprExtensions
{
    /// <summary>
    /// Creates or updates a recurring schedule that fires <typeparamref name="T"/> every <paramref name="timeSpan"/> interval.
    /// </summary>
    public static async Task<RecurringScheduleReceipt> RecurringAsync<T>(
        this IJobMasterScheduler scheduler,
        TimeSpan timeSpan,
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
        var compiledExpr = new TimeSpanIntervalCompiledExpr
        {
            Interval = timeSpan,
            Expression = timeSpan.ToString()
        };
        
        return await scheduler.RecurringAsync<T>(compiledExpr, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
    }
    
    /// <summary>
    /// Synchronous version of <see cref="RecurringAsync{T}(IJobMasterScheduler, TimeSpan, IWriteableMessageData, string, JobMasterPriority?, string, TimeSpan?, int?, IWritableMetadata, DateTime?, DateTime?)"/>.
    /// </summary>
    public static RecurringScheduleReceipt Recurring<T>(
        this IJobMasterScheduler scheduler,
        TimeSpan timeSpan,
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
        var compiledExpr = new TimeSpanIntervalCompiledExpr
        {
            Interval = timeSpan,
            Expression = timeSpan.ToString()
        };
        
        return scheduler.Recurring<T>(compiledExpr, data, clusterId, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
    }
    
    /// <summary>Adds a fixed-interval recurring schedule definition to <paramref name="collection"/>.</summary>
    public static RecurringScheduleDefinitionCollection Add<Th>(
        this RecurringScheduleDefinitionCollection collection,
        TimeSpan timeSpan,
        string? defId = null,
        JobMasterPriority? priority = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null) where Th : class, IJobMasterHandler
    {
        collection.Add<Th>(TimeSpanIntervalExprCompiler.TypeId, timeSpan.ToString(), defId, priority, timeout, maxNumberOfRetries, metadata, startAfter, endBefore);
        return collection;
    }
}