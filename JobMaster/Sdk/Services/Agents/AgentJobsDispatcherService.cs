using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Services.Agents;

internal class AgentJobsDispatcherService : JobMasterClusterAwareComponent, IAgentJobsDispatcherService
{
    private IAgentComponentFactory agentComponentFactory = null!;
    private readonly IJobMasterRuntime jobMasterRuntime;
    private readonly IJobMasterLogger logger;

    public AgentJobsDispatcherService(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IAgentComponentFactory agentComponentFactory,
        IJobMasterRuntime jobMasterRuntime,
        IJobMasterLogger logger) : base(clusterConnectionConfig)
    {
        this.agentComponentFactory = agentComponentFactory;
        this.jobMasterRuntime = jobMasterRuntime;
        this.logger = logger;
    }

    public string AddSavePendingJob(JobRawModel jobRaw)
    {
        ValidateJobAssignedToBucket(jobRaw);

        var repository = GetJobDispatcherRepository(jobRaw.AgentConnectionId!);
        var throttler = GetOperationLimiter(jobRaw.AgentConnectionId!);
        return throttler.Exec(() => repository.PushSavePendingJob(jobRaw));
    }

    public async Task<string> AddSavePendingJobAsync(JobRawModel jobRaw)
    {
        ValidateJobAssignedToBucket(jobRaw);

        var repository = GetJobDispatcherRepository(jobRaw.AgentConnectionId!);
        var throttler = GetOperationLimiter(jobRaw.AgentConnectionId!);
        return await throttler.ExecAsync(() => repository.PushSavePendingJobAsync(jobRaw));
    }

    public async Task<IList<string>> BulkAddSavePendingJobAsync(List<JobRawModel> jobRawModels)
    {
        IDictionary<string, IList<JobRawModel>> jobsByBucket = new Dictionary<string, IList<JobRawModel>>();
        foreach (var jobRawModel in jobRawModels)
        {
            ValidateJobAssignedToBucket(jobRawModel);

            if (!jobsByBucket.ContainsKey(jobRawModel.BucketId!))
            {
                jobsByBucket[jobRawModel.BucketId!] = new List<JobRawModel>();
            }

            jobsByBucket[jobRawModel.BucketId!].Add(jobRawModel);
        }

        var results = new List<string>();
        foreach (var item in jobsByBucket)
        {
            if (item.Value.Count == 0)
            {
                continue;
            }

            var bucketId = item.Key;
            var agentConnectionId = item.Value[0].AgentConnectionId!;
            var repository = GetJobDispatcherRepository(agentConnectionId);
            var throttler = GetOperationLimiter(agentConnectionId);
            var partitions = item.Value.Partition(JobMasterConstants.MaxBatchSizeForBulkOperation);

            foreach (var partition in partitions)
            {
                logger.Debug($"Bulk scheduling jobs. partition size: {partition.Count} for bucket {bucketId}",
                    JobMasterLogSubjectType.Job, partition.First().Id);
                var partitionResult = await throttler.ExecAsync(() =>
                    repository.BulkPushSavePendingJobAsync(bucketId, partition.ToList()));
                results.AddRange(partitionResult);
            }
        }

        return results;
    }

    public string AddSavePendingRecur(RecurringScheduleRawModel recurringScheduleRaw)
    {
        ValidateRecurringScheduleAssignedToBucket(recurringScheduleRaw);

        var repository = GetJobDispatcherRepository(recurringScheduleRaw.AgentConnectionId!);
        var throttler = GetOperationLimiter(recurringScheduleRaw.AgentConnectionId!);
        return throttler.Exec(() => repository.PushForSaving(recurringScheduleRaw));
    }

    public async Task<string> AddSavePendingRecurAsync(RecurringScheduleRawModel recurringScheduleRaw)
    {
        ValidateRecurringScheduleAssignedToBucket(recurringScheduleRaw);

        var repository = GetJobDispatcherRepository(recurringScheduleRaw.AgentConnectionId!);
        var throttler = GetOperationLimiter(recurringScheduleRaw.AgentConnectionId!);
        return await throttler.ExecAsync(() => repository.PushForSavingAsync(recurringScheduleRaw));
    }

    public async Task<string> AddForProcessingAsync(JobRawModel jobRaw)
    {
        ValidateJobAssignedToBucket(jobRaw);

        var repository = GetJobDispatcherRepository(jobRaw.AgentConnectionId!);

        var throttler = GetOperationLimiter(jobRaw.AgentConnectionId!);
        return await throttler.ExecAsync(() => repository.PushForProcessingAsync(jobRaw));
    }

    public async Task<IList<JobRawModel>> DispatchForProcessingAsync(AgentConnectionId agentConnectionId, string bucketId,
        int numberOfJobs, DateTime? scheduleTo)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        var throttler = GetOperationLimiter(agentConnectionId);
        return await throttler.ExecAsync(() => repository.DispatchForProcessingAsync(bucketId, numberOfJobs, scheduleTo));
    }

    public async Task<IList<JobRawModel>> DispatchSavePendingJobsAsync(AgentConnectionId agentConnectionId,
        string bucketId, int numberOfJobs)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        var throttler = GetOperationLimiter(agentConnectionId);
        return await throttler.ExecAsync(() => repository.DispatchSavePendingJobsAsync(bucketId, numberOfJobs));
    }

    public async Task<IList<RecurringScheduleRawModel>> DequeueSavePendingRecurAsync(
        AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        var throttler = GetOperationLimiter(agentConnectionId);
        return await throttler.ExecAsync(() => repository.DequeueSavePendingRecurAsync(bucketId, numberOfJobs));
    }

    public async Task<bool> HasJobsAsync(AgentConnectionId agentConnectionId, string bucketId)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        var throttler = GetOperationLimiter(agentConnectionId);
        return await throttler.ExecAsync(() => repository.HasJobsAsync(bucketId!));
    }

    public async Task CreateBucketAsync(AgentConnectionId agentConnectionId, string bucketId)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId!);
        var throttler = GetOperationLimiter(agentConnectionId!);
        await throttler.ExecAsync(() => repository.CreateBucketAsync(bucketId!));
    }

    public async Task DestroyBucketAsync(AgentConnectionId agentConnectionId, string bucketId)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId!);
        var throttler = GetOperationLimiter(agentConnectionId!);
        await throttler.ExecAsync(() => repository.DestroyBucketAsync(bucketId!));
    }

    private IAgentJobsDispatcherRepository GetJobDispatcherRepository(AgentConnectionId agentConnectionId)
    {
        return agentComponentFactory.GetRepository(agentConnectionId);
    }

    private OperationThrottler GetOperationLimiter(AgentConnectionId agentConnectionId)
    {
        return jobMasterRuntime.GetOperationLimiterForAgent(ClusterConnConfig.ClusterId, agentConnectionId.IdValue);
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