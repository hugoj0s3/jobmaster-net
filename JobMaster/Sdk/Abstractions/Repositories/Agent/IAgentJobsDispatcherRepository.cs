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
    
    Task<IList<JobRawModel>> DispatchForProcessingAsync(string bucketId, int numberOfJobs, DateTime? scheduleTo);
    
    Task<IList<JobRawModel>> DispatchSavePendingJobsAsync(string bucketId, int numberOfJobs);
    
    Task<IList<RecurringScheduleRawModel>> DequeueSavePendingRecurAsync(string bucketId, int numberOfJobs);
    Task<bool> HasJobsAsync(string bucketId);
    
    Task CreateBucketAsync(string bucketId);
    Task DestroyBucketAsync(string bucketId);
    
    void Initialize(JobMasterAgentConnectionConfig agentConnConfig);
    
    string AgentRepoTypeId { get; }
    
    bool IsAutoDequeueForSaving { get; }
    bool IsAutoDequeueForProcessing { get; }
}

internal interface IAgentJobsDispatcherRepository<TSavePending, TProcessing> : IAgentJobsDispatcherRepository
    where TSavePending : class, IAgentRawMessagesDispatcherRepository
    where TProcessing : class, IAgentRawMessagesDispatcherRepository
{
}

