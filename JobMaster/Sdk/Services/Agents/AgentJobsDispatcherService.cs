using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Agents;

internal class AgentJobsDispatcherService : JobMasterClusterAwareComponent, IAgentJobsDispatcherService
{
    private IAgentComponentFactory agentComponentFactory = null!;
    private readonly IJobMasterLogger logger;

    public AgentJobsDispatcherService(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IAgentComponentFactory agentComponentFactory,
        IJobMasterLogger logger) : base(clusterConnectionConfig)
    {
        this.agentComponentFactory = agentComponentFactory;
        this.logger = logger;
    }

    public string AddSavePendingJob(JobRawModel jobRaw, OperationThrottler? throttler = null)
    {
        ValidateJobAssignedToBucket(jobRaw);

        var repository = GetJobDispatcherRepository(jobRaw.AgentConnectionId!);
        return ResolveThrottler(throttler, jobRaw.AgentConnectionId!).Exec(() => repository.PushSavePendingJob(jobRaw));
    }

    public async Task<string> AddSavePendingJobAsync(JobRawModel jobRaw, OperationThrottler? throttler = null)
    {
        ValidateJobAssignedToBucket(jobRaw);

        var repository = GetJobDispatcherRepository(jobRaw.AgentConnectionId!);
        return await ResolveThrottler(throttler, jobRaw.AgentConnectionId!).ExecAsync(() => repository.PushSavePendingJobAsync(jobRaw));
    }

    public async Task<IList<string>> BulkAddSavePendingJobAsync(AgentConnectionId agentConnectionId, string bucketId, List<JobRawModel> jobRawModels, OperationThrottler? throttler = null)
    {
        if (jobRawModels.Count == 0)
        {
            return new List<string>();
        }

        foreach (var jobRawModel in jobRawModels)
        {
            ValidateJobAssignedToBucket(jobRawModel);
        }

        var repository = GetJobDispatcherRepository(agentConnectionId);

        logger.Debug($"Bulk scheduling jobs. partition size: {jobRawModels.Count} for bucket {bucketId}",
            JobMasterLogCategory.Job, jobRawModels[0].Id);
        return await ResolveThrottler(throttler, agentConnectionId).ExecAsync(() => repository.BulkPushSavePendingJobAsync(bucketId, jobRawModels));
    }

    public string AddSavePendingRecur(RecurringScheduleRawModel recurringScheduleRaw, OperationThrottler? throttler = null)
    {
        ValidateRecurringScheduleAssignedToBucket(recurringScheduleRaw);

        var repository = GetJobDispatcherRepository(recurringScheduleRaw.AgentConnectionId!);
        return ResolveThrottler(throttler, recurringScheduleRaw.AgentConnectionId!).Exec(() => repository.PushForSaving(recurringScheduleRaw));
    }

    public async Task<string> AddSavePendingRecurAsync(RecurringScheduleRawModel recurringScheduleRaw, OperationThrottler? throttler = null)
    {
        ValidateRecurringScheduleAssignedToBucket(recurringScheduleRaw);

        var repository = GetJobDispatcherRepository(recurringScheduleRaw.AgentConnectionId!);
        return await ResolveThrottler(throttler, recurringScheduleRaw.AgentConnectionId!).ExecAsync(() => repository.PushForSavingAsync(recurringScheduleRaw));
    }

    public async Task<string> AddForProcessingAsync(JobRawModel jobRaw, OperationThrottler? throttler = null)
    {
        ValidateJobAssignedToBucket(jobRaw);

        var repository = GetJobDispatcherRepository(jobRaw.AgentConnectionId!);
        return await ResolveThrottler(throttler, jobRaw.AgentConnectionId!).ExecAsync(() => repository.PushForProcessingAsync(jobRaw));
    }

    public async Task<IList<string>> BulkAddForProcessingAsync(AgentConnectionId agentConnectionId, string bucketId, List<JobRawModel> jobRawModels, OperationThrottler? throttler = null)
    {
        if (jobRawModels.Count == 0)
        {
            return new List<string>();
        }

        foreach (var jobRawModel in jobRawModels)
        {
            ValidateJobAssignedToBucket(jobRawModel);
        }

        var repository = GetJobDispatcherRepository(agentConnectionId);

        logger.Debug($"Bulk dispatching jobs for processing. partition size: {jobRawModels.Count} for bucket {bucketId}",
            JobMasterLogCategory.Job, jobRawModels[0].Id);
        return await ResolveThrottler(throttler, agentConnectionId).ExecAsync(() => repository.BulkPushForProcessingAsync(bucketId, jobRawModels));
    }

    public async Task<IList<JobRawModel>> PullForProcessingAsync(AgentConnectionId agentConnectionId, string bucketId,
        int numberOfJobs, DateTime? scheduleTo, OperationThrottler? throttler = null)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        return await ResolveThrottler(throttler, agentConnectionId).ExecAsync(() => repository.PullForProcessingAsync(bucketId, numberOfJobs, scheduleTo));
    }

    public async Task<IList<JobRawModel>> PullSavePendingJobsAsync(AgentConnectionId agentConnectionId,
        string bucketId, int numberOfJobs, OperationThrottler? throttler = null)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        return await ResolveThrottler(throttler, agentConnectionId).ExecAsync(() => repository.PullSavePendingJobsAsync(bucketId, numberOfJobs));
    }

    public async Task<IList<RecurringScheduleRawModel>> PullSavePendingRecurAsync(
        AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs, OperationThrottler? throttler = null)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        return await ResolveThrottler(throttler, agentConnectionId).ExecAsync(() => repository.PullSavePendingRecurAsync(bucketId, numberOfJobs));
    }

    public async Task<bool> HasJobsAsync(AgentConnectionId agentConnectionId, string bucketId, OperationThrottler? throttler = null)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        return await ResolveThrottler(throttler, agentConnectionId).ExecAsync(() => repository.HasJobsAsync(bucketId!));
    }

    public async Task CreateBucketAsync(AgentConnectionId agentConnectionId, string bucketId, OperationThrottler? throttler = null)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId!);
        await ResolveThrottler(throttler, agentConnectionId!).ExecAsync(() => repository.CreateBucketAsync(bucketId!));
    }

    public async Task DestroyBucketAsync(AgentConnectionId agentConnectionId, string bucketId, OperationThrottler? throttler = null)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId!);
        await ResolveThrottler(throttler, agentConnectionId!).ExecAsync(() => repository.DestroyBucketAsync(bucketId!));
    }

    private IAgentJobsDispatcherRepository GetJobDispatcherRepository(AgentConnectionId agentConnectionId)
    {
        return agentComponentFactory.GetRepository(agentConnectionId);
    }

    // Every dispatcher call (scheduling and internal-process alike) defaults to this same
    // internal-process, per-agent-connection throttler unless a caller passes its own -- e.g.
    // JobMasterSchedulerClusterAware passes an unbounded throttler explicitly, since it's always
    // external-facing and shouldn't be artificially gated by app-level throttling.
    private OperationThrottler ResolveThrottler(OperationThrottler? throttler, AgentConnectionId agentConnectionId)
    {
        return throttler ?? OperationThrottlerSettingsFactory.GetInternalAgentThrottler(agentConnectionId.IdValue);
    }

    private void ValidateJobAssignedToBucket(JobRawModel jobRaw)
    {
        if (!jobRaw.AgentConnectionId.IsNotNullAndValid() ||
            string.IsNullOrEmpty(jobRaw.BucketId) ||
            string.IsNullOrEmpty(jobRaw.AgentWorkerId) ||
            !jobRaw.HostId.IsNotNullAndValid())
        {
            throw new InvalidOperationException(
                "Job is not fully assigned to a bucket. AgentConnectionId, BucketId, AgentWorkerId, and HostId are required.");
        }
    }

    private void ValidateRecurringScheduleAssignedToBucket(RecurringScheduleRawModel recurringScheduleRaw)
    {
        if (!recurringScheduleRaw.AgentConnectionId.IsNotNullAndValid() ||
            string.IsNullOrEmpty(recurringScheduleRaw.BucketId) ||
            string.IsNullOrEmpty(recurringScheduleRaw.AgentWorkerId) ||
            !recurringScheduleRaw.HostId.IsNotNullAndValid())
        {
            throw new InvalidOperationException(
                "Recurring schedule is not assigned to a bucket. AgentConnectionId and BucketId are required.");
        }
    }
}