using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Background;

internal interface IWorkerClusterOperations : IJobMasterClusterAwareService
{
    Task DispatchJobToBucketAsync(IJobMasterBackgroundAgentWorker backgroundAgentWorker, JobRawModel jobRaw, BucketModel bucket);
    void MarkAsHeldOnMaster(Guid jobId);
    void CancelJob(Guid jobId);
    void Upsert(JobRawModel jobRawModel);
    Task UpsertAsync(JobRawModel jobRawModel);
    
    void Update(JobRawModel jobRawModel, JobExecution? jobExecution = null);
    Task UpdateAsync(JobRawModel jobRawModel, JobExecution? jobExecution = null);
    
    Task AddJobExecutionAsync(JobExecution jobExecution);
    
    void Upsert(RecurringScheduleRawModel recurringScheduleRawModel);
    Task MarkBucketAsLostAsync(BucketModel bucket);
    Task MarkBucketAsLostAsync(string bucketId);
    Task MarkBucketAsLostIfNotDrainingAsync(string bucketId);
    Task MarkBucketAsReadyToDeleteAsync(string bucketId);
    Task MarkBucketAsReadyToDrainAsync(string bucketId);
    void MarkBucketAsLost(BucketModel bucket);
    Task<int> CountActiveCoordinatorWorkersAsync();
    Task<int> CountWorkersAsync();
    void CancelRecurringSchedule(Guid id);
    
    Task ExecWithRetryAsync(Action<IWorkerClusterOperations> func, int maxRetries = 5, int millisecondsToDelay = 200);
    Task ExecWithRetryAsync(Func<IWorkerClusterOperations, Task> func, int maxRetries = 5, int millisecondsToDelay = 200);
    Task<T> ExecWithRetryAsync<T>(Func<IWorkerClusterOperations, Task<T>> func, int maxRetries = 5, int millisecondsToDelay = 200);

    Task BulkUpdateAsync(BulkJobUpdateRequest request);
    Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs);

    Task AddAsync(JobRawModel job);
}