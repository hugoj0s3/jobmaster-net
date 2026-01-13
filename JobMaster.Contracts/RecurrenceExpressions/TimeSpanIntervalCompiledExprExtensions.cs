using JobMaster.Contracts.Models;

namespace JobMaster.Contracts.RecurrenceExpressions;

public static class TimeSpanIntervalCompiledExprExtensions
{
    public static async Task<RecurringScheduleContext> RecurringAsync<T>(
        this IJobMasterScheduler scheduler, 
        TimeSpan timeSpan,
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
        var compiledExpr = new TimeSpanIntervalCompiledExpr
        {
            Interval = timeSpan,
            Expression = timeSpan.ToString()
        };
        
        return await scheduler.RecurringAsync<T>(compiledExpr, data, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, clusterId);
    }
    
    public static RecurringScheduleContext Recurring<T>(
        this IJobMasterScheduler scheduler, 
        TimeSpan timeSpan,
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
        var compiledExpr = new TimeSpanIntervalCompiledExpr
        {
            Interval = timeSpan,
            Expression = timeSpan.ToString()
        };
        
        return scheduler.Recurring<T>(compiledExpr, data, priority, workerLane, timeout, maxNumberOfRetries, metadata, startAfter, endBefore, clusterId);
    }
}