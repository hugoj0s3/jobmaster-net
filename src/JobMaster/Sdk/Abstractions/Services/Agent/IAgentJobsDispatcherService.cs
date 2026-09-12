using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services.Agent;

/// <summary>
/// Every method takes an optional <see cref="OperationThrottler"/>. Leave it null to use the
/// dispatcher's own internal-process throttler (the existing per-agent-connection limiter) --
/// pass one explicitly to override it, e.g. an unbounded throttler for a caller that is always
/// external-facing and shouldn't be artificially gated by app-level throttling (see
/// <c>JobMasterSchedulerClusterAware</c>).
/// </summary>
internal interface IAgentJobsDispatcherService : IJobMasterClusterAwareService
{
    string AddSavePendingJob(JobRawModel jobRaw, OperationThrottler? throttler = null);

    Task<string> AddSavePendingJobAsync(JobRawModel jobRaw, OperationThrottler? throttler = null);

    Task<IList<string>> BulkAddSavePendingJobAsync(AgentConnectionId agentConnectionId, string bucketId, List<JobRawModel> jobRawModels, OperationThrottler? throttler = null);
    Task<IList<string>> BulkAddForProcessingAsync(AgentConnectionId agentConnectionId, string bucketId, List<JobRawModel> jobRawModels, OperationThrottler? throttler = null);

    string AddSavePendingRecur(RecurringScheduleRawModel recurringScheduleRaw, OperationThrottler? throttler = null);

    Task<string> AddSavePendingRecurAsync(RecurringScheduleRawModel recurringScheduleRaw, OperationThrottler? throttler = null);

    Task<string> AddForProcessingAsync(JobRawModel jobRaw, OperationThrottler? throttler = null);

    Task<IList<JobRawModel>> PullForProcessingAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs, DateTime? scheduleTo, OperationThrottler? throttler = null);

    Task<IList<JobRawModel>> PullSavePendingJobsAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs, OperationThrottler? throttler = null);

    Task<IList<RecurringScheduleRawModel>> PullSavePendingRecurAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs, OperationThrottler? throttler = null);

    Task<bool> HasJobsAsync(AgentConnectionId agentConnectionId, string bucketId, OperationThrottler? throttler = null);

    Task CreateBucketAsync(AgentConnectionId agentConnectionId, string bucketId, OperationThrottler? throttler = null);

    Task DestroyBucketAsync(AgentConnectionId agentConnectionId, string bucketId, OperationThrottler? throttler = null);
}