using System.ComponentModel;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Repositories.Agent;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IAgentJobsDispatcherRepository : IJobMasterClusterAwareComponent
{
    string PushToSaving(RecurringScheduleRawModel recurringScheduleRaw);
    Task<string> PushToSavingAsync(RecurringScheduleRawModel recurringScheduleRaw);
    
    string PushSavePendingJob(JobRawModel jobRaw);
    Task<string> PushSavePendingJobAsync(JobRawModel jobRaw);
    
    Task<IList<string>> BulkPushSavePendingJobAsync(string bucketId, IList<JobRawModel> jobRaw);
    
    string PushToProcessing(JobRawModel jobRaw);
    Task<string> PushToProcessingAsync(JobRawModel jobRaw);
    
    Task<IList<JobRawModel>> DequeueToProcessingAsync(string bucketId, int numberOfJobs, DateTime? scheduleTo);
    
    Task<IList<JobRawModel>> DequeueSavePendingJobsAsync(string bucketId, int numberOfJobs);
    
    Task<IList<RecurringScheduleRawModel>> DequeueSavePendingRecurAsync(string bucketId, int numberOfJobs);
    Task<bool> HasJobsAsync(string bucketId);
    
    Task CreateBucketAsync(string bucketId);
    Task DestroyBucketAsync(string bucketId);
    
    void Initialize(JobMasterAgentConnectionConfig agentConnConfig);
    
    string AgentRepoTypeId { get; }
    
    bool IsAutoDequeueForSaving { get; }
    bool IsAutoDequeueForProcessing { get; }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IAgentJobsDispatcherRepository<TSavePending, TProcessing> : IAgentJobsDispatcherRepository
    where TSavePending : class, IAgentRawMessagesDispatcherRepository
    where TProcessing : class, IAgentRawMessagesDispatcherRepository
{
}

