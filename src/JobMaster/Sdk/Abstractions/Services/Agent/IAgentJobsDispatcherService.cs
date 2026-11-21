using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services.Agent;

/// <summary>
/// The message methods take an optional <see cref="OperationThrottler"/>. Leave it null to use the
/// dispatcher's own internal-process throttler (the existing per-agent-connection limiter) --
/// pass one explicitly to override it, e.g. an unbounded throttler for a caller that is always
/// external-facing and shouldn't be artificially gated by app-level throttling (see
/// <c>JobMasterSchedulerClusterAware</c>). The bucket operations (<c>HasJobsAsync</c>,
/// <c>CreateBucketAsync</c>, <c>DestroyBucketAsync</c>) always use the per-connection bucket throttler.
/// </summary>
internal interface IAgentJobsDispatcherService : IJobMasterClusterAwareService
{
    string PushSavePendingJob(JobRawModel jobRaw, OperationThrottler? throttler = null);

    Task<string> PushSavePendingJobAsync(JobRawModel jobRaw, OperationThrottler? throttler = null);

    Task<IList<string>> BulkPushSavePendingJobAsync(AgentConnectionId agentConnectionId, string bucketId, List<JobRawModel> jobRawModels, OperationThrottler? throttler = null);
    Task<IList<string>> BulkPushForProcessingAsync(AgentConnectionId agentConnectionId, string bucketId, List<JobRawModel> jobRawModels, OperationThrottler? throttler = null);

    string PushSavePendingRecur(RecurringScheduleRawModel recurringScheduleRaw, OperationThrottler? throttler = null);

    Task<string> PushSavePendingRecurAsync(RecurringScheduleRawModel recurringScheduleRaw, OperationThrottler? throttler = null);

    Task<string> PushForProcessingAsync(JobRawModel jobRaw, OperationThrottler? throttler = null);

    Task<IList<JobRawModel>> PullForProcessingAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs, DateTime? scheduleTo, OperationThrottler? throttler = null);

    /// <summary>
    /// Leases pending-save jobs: the first half of lease + ack, explained on
    /// <see cref="Repositories.Agent.IAgentJobsDispatcherRepository.LeaseSavePendingJobsAsync"/>. Ack them with
    /// <see cref="AckSavePendingJobsAsync"/> once on master; an unacked one is leased again after its lease.
    /// </summary>
    Task<IList<(JobRawModel Job, string AckKey)>> LeaseSavePendingJobsAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs, TimeSpan lease, OperationThrottler? throttler = null);

    /// <summary>
    /// Acks leased jobs (removes their messages): the second half of <see cref="LeaseSavePendingJobsAsync"/>. Only
    /// for jobs now on master.
    /// </summary>
    Task AckSavePendingJobsAsync(AgentConnectionId agentConnectionId, string bucketId, IList<string> ackKeys, OperationThrottler? throttler = null);

    /// <summary>
    /// Leases pending-save recurring schedules: lease + ack as for <see cref="LeaseSavePendingJobsAsync"/>, acked
    /// with <see cref="AckSavePendingRecurAsync"/>.
    /// </summary>
    Task<IList<(RecurringScheduleRawModel Schedule, string AckKey)>> LeaseSavePendingRecurAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfSchedules, TimeSpan lease, OperationThrottler? throttler = null);

    /// <summary>
    /// Acks leased recurring schedules (removes their messages): the second half of
    /// <see cref="LeaseSavePendingRecurAsync"/>. Only for schedules now on master.
    /// </summary>
    Task AckSavePendingRecurAsync(AgentConnectionId agentConnectionId, string bucketId, IList<string> ackKeys, OperationThrottler? throttler = null);

    Task<bool> HasJobsAsync(AgentConnectionId agentConnectionId, string bucketId);

    Task CreateBucketAsync(AgentConnectionId agentConnectionId, string bucketId);

    Task DestroyBucketAsync(AgentConnectionId agentConnectionId, string bucketId);
}