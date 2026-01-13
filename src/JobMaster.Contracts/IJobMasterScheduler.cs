using JobMaster.Contracts.Models;
using JobMaster.Contracts.RecurrenceExpressions;

namespace JobMaster.Contracts;

public interface IJobMasterScheduler
{
    JobContext OnceNow<T>(
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobHandler;

    JobContext OnceAt<T>(
        DateTime dateTime,
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobHandler;

    JobContext OnceAfter<T>(
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobHandler;

    Task<JobContext> OnceNowAsync<T>(
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobHandler;

    Task<JobContext> OnceAtAsync<T>(
        DateTime scheduledAt,
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobHandler;

    Task<JobContext> OnceAfterAsync<T>(
        TimeSpan after,
        IWriteableMessageData? msgData = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        string? clusterId = null) where T : IJobHandler;
    
    RecurringScheduleContext Recurring<T>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null) where T : IJobHandler;

    Task<RecurringScheduleContext> RecurringAsync<T>(
        IRecurrenceCompiledExpr expression,
        IWriteableMessageData? data = null,
        JobMasterPriority? priority = null,
        string? workerLane = null,
        TimeSpan? timeout = null,
        int? maxNumberOfRetries = null,
        IWritableMetadata? metadata = null,
        DateTime? startAfter = null,
        DateTime? endBefore = null,
        string? clusterId = null) where T : IJobHandler;
    
    RecurringScheduleContext Recurring<T>(
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
        string? clusterId = null) where T : IJobHandler;

    Task<RecurringScheduleContext> RecurringAsync<T>(
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
        string? clusterId = null) where T : IJobHandler;
    
    Task<bool> CancelJobAsync(Guid jobId, string? clusterId = null);

    bool TryCancelJob(Guid id, string? clusterId = null);

    Task<bool> TryCancelRecurringAsync(Guid id, string? clusterId = null);

    bool CancelRecurring(Guid id, string? clusterId = null);
    
    Task<bool> ReScheduleAsync(Guid jobId, DateTime scheduledAt, string? clusterId = null);

    bool ReSchedule(Guid jobId, DateTime scheduledAt, string? clusterId = null);
}