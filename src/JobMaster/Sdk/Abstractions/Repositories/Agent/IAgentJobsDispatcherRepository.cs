using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Repositories.Agent;

internal interface IAgentJobsDispatcherRepository : IJobMasterClusterAwareComponent
{
    string PushForSaving(RecurringScheduleRawModel recurringScheduleRaw);
    Task<string> PushForSavingAsync(RecurringScheduleRawModel recurringScheduleRaw);
    
    string PushSavePendingJob(JobRawModel jobRaw);
    Task<string> PushSavePendingJobAsync(JobRawModel jobRaw);
    
    Task<IList<string>> BulkPushSavePendingJobAsync(string bucketId, IList<JobRawModel> jobRaw);
    
    string PushForProcessing(JobRawModel jobRaw);
    Task<string> PushForProcessingAsync(JobRawModel jobRaw);

    Task<IList<string>> BulkPushForProcessingAsync(string bucketId, IList<JobRawModel> jobRaw);

    Task<IList<JobRawModel>> PullForProcessingAsync(string bucketId, int numberOfJobs, DateTime? scheduleTo);
    
    /// <summary>
    /// A message leased <see cref="JobMasterConstants.MaxSavePendingLeaseCount"/> times is not returned: it is moved
    /// to the bucket's terminated address and logged. Same for <see cref="LeaseSavePendingRecurAsync"/>.
    /// </summary>
    Task<IList<(JobRawModel Job, string AckKey)>> LeaseSavePendingJobsAsync(string bucketId, int numberOfJobs, TimeSpan lease);

    Task AckSavePendingJobsAsync(string bucketId, IList<string> ackKeys);
    
    Task<IList<(RecurringScheduleRawModel Schedule, string AckKey)>> LeaseSavePendingRecurAsync(string bucketId, int numberOfSchedules, TimeSpan lease);

    Task AckSavePendingRecurAsync(string bucketId, IList<string> ackKeys);
    Task<bool> HasJobsAsync(string bucketId);
    
    Task CreateBucketAsync(string bucketId);
    Task DestroyBucketAsync(string bucketId);
    
    void Initialize(JobMasterAgentConnectionConfig agentConnConfig);
    
    string AgentRepoTypeId { get; }
    
    bool IsPollingBasedForSaving { get; }
    bool IsPollingBasedForProcessing { get; }
}
