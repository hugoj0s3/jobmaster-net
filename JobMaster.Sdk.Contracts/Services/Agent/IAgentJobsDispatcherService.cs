using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Models.Agents;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;

namespace JobMaster.Sdk.Contracts.Services.Agent;

public interface IAgentJobsDispatcherService : IJobMasterClusterAwareService
{
    string AddSavePendingJob(JobRawModel jobRaw);

    Task<string> AddSavePendingJobAsync(JobRawModel jobRaw);
    
    Task<IList<string>> BulkAddSavePendingJobAsync(List<JobRawModel> jobRawModels);
    
    string AddSavePendingRecur(RecurringScheduleRawModel recurringScheduleRaw);
    
    Task<string> AddSavePendingRecurAsync(RecurringScheduleRawModel recurringScheduleRaw);
    
    string AddToProcessing(string workerId, AgentConnectionId agentConnectionId, string bucketId, JobRawModel jobRaw);
    Task<string> AddToProcessingAsync(string workerId, AgentConnectionId agentConnectionId, string bucketId, JobRawModel jobRaw);
    
    Task<IList<JobRawModel>> DequeueToProcessingAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs, DateTime? scheduleTo);
    
    Task<IList<JobRawModel>> DequeueSavePendingJobsAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs);
    
    Task<IList<RecurringScheduleRawModel>> DequeueSavePendingRecurAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs);
    
    Task<bool> HasJobsAsync(AgentConnectionId agentConnectionId, string bucketId);
    
    Task CreateBucketAsync(AgentConnectionId agentConnectionId, string bucketId);
    
    Task DestroyBucketAsync(AgentConnectionId agentConnectionId, string bucketId);
}