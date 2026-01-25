using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Background;

internal interface IWorkerClusterOperations : IJobMasterClusterAwareService
{
    Task AssignJobToBucketFromHeldOnMasterOrSavePendingAsync(IJobMasterBackgroundAgentWorker backgroundAgentWorker, JobRawModel jobRaw, BucketModel bucket);
    void MarkAsHeldOnMaster(Guid jobId);
    void CancelJob(Guid jobId);
    void Upsert(JobRawModel jobRawModel);
    Task UpsertAsync(JobRawModel jobRawModel);
    
    void Upsert(RecurringScheduleRawModel jobRawModel);
    Task MarkBucketAsLostAsync(BucketModel bucket);
    Task MarkBucketAsLostAsync(string bucketId);
    Task MarkBucketAsLostIfNotDrainingAsync(string bucketId);
    void MarkBucketAsLost(BucketModel bucket);
    Task<int> CountActiveCoordinatorWorkersAsync();
    void CancelRecurringSchedule(Guid id);
    
    Task ExecWithRetryAsync(Action<IWorkerClusterOperations> func, int maxRetries = 5, int millisecondsToDelay = 200);
    Task ExecWithRetryAsync(Func<IWorkerClusterOperations, Task> func, int maxRetries = 5, int millisecondsToDelay = 200);

    Task AddAsync(JobRawModel job);
    
    void Insert(JobRawModel job);
}