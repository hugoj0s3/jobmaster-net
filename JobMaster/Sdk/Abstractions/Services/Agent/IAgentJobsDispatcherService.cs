using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services.Agent;

internal interface IAgentJobsDispatcherService : IJobMasterClusterAwareService
{
    string AddSavePendingJob(JobRawModel jobRaw, bool notifyAgent = true);

    Task<string> AddSavePendingJobAsync(JobRawModel jobRaw, bool notifyAgent = true);
    
    Task<IList<string>> BulkAddSavePendingJobAsync(AgentConnectionId agentConnectionId, string bucketId, List<JobRawModel> jobRawModels);
    Task<IList<string>> BulkAddForProcessingAsync(AgentConnectionId agentConnectionId, string bucketId, List<JobRawModel> jobRawModels);
    
    string AddSavePendingRecur(RecurringScheduleRawModel recurringScheduleRaw);
    
    Task<string> AddSavePendingRecurAsync(RecurringScheduleRawModel recurringScheduleRaw);
    
    Task<string> AddForProcessingAsync(JobRawModel jobRaw);

    Task<IList<JobRawModel>> PullForProcessingAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs, DateTime? scheduleTo);
    
    Task<IList<JobRawModel>> PullSavePendingJobsAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs);
    
    Task<IList<RecurringScheduleRawModel>> PullSavePendingRecurAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs);
    
    Task<bool> HasJobsAsync(AgentConnectionId agentConnectionId, string bucketId);
    
    Task CreateBucketAsync(AgentConnectionId agentConnectionId, string bucketId);
    
    Task DestroyBucketAsync(AgentConnectionId agentConnectionId, string bucketId);
}