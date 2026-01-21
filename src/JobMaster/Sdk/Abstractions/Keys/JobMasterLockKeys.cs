using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Keys;

[EditorBrowsable(EditorBrowsableState.Never)]
public class JobMasterLockKeys : JobMasterKeyManager
{
    public JobMasterLockKeys(string clusterId) : base("Lock", clusterId)
    {
    }

    public string RecurringScheduleCancellingLock(Guid recurringScheduleId) => 
        CreateKey($"RecurringScheduleCancelling:{recurringScheduleId:N}");

    public string BucketAssignerLock(int lockId) => CreateKey($"BucketAssignerLock:{lockId}");
    
    public string MarkBucketAsLostLock(string bucketId) => CreateKey($"MarkBucketAsLost:{bucketId}");
    
    public string BucketRunnerLock() => CreateKey($"BucketRunnerLock");
    public string BucketLock(string bucketId) => CreateKey($"BucketLock:{bucketId}");
    
    public string ProcessDeadlineTimeoutLock(int lockId) => CreateKey($"ProcessDeadlineTimeout:{lockId}");
    
    public string WorkerGracefulStopLock(string workerId) => CreateKey($"WorkerFriendlyStop:{workerId}");
    public string WorkerImmediateStopLock(string workerId) => CreateKey($"WorkerImmediateStop:{workerId}");

    public string RecurringSchedulerLock(int lockId) => CreateKey($"RecurringSchedulerLock:{lockId}");

    public string RecurringScheduleProcessingLock(Guid recurringScheduleId) => CreateKey($"RecurringScheduleProcessing:{recurringScheduleId:N}");

    public string GenericRecordsCleanupLock() => CreateKey("GenericRecordsCleanup");
    
    public string JobsCleanupLock() => CreateKey("JobsCleanup");
    
    public string RecurringSchedulesCleanupLock() => CreateKey("RecurringSchedulesCleanup");

    public string RecurringSchedulePlan(Guid id) => CreateKey($"RecurringSchedulePlan:{id:N}");
    
    public string RecurringScheduleUpsertStatic(string staticId) => CreateKey($"RecurringScheduleUpsertStatic:{staticId}");

    public string StaticDefinitionsKeepAliveLock() => CreateKey("StaticDefinitionsKeepAlive");
    
    public string LockCleanupLock() => CreateKey("LockCleanup");
}