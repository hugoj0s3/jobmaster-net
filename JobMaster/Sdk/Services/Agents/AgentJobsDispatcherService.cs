using JobMaster.Internals;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Agents;

internal class AgentJobsDispatcherService : JobMasterClusterAwareComponent, IAgentJobsDispatcherService
{
    private IAgentJobsDispatcherRepositoryFactory agentJobsDispatcherRepositoryFactory = null!;
    private readonly IJobMasterRuntime jobMasterRuntime;
    private readonly IJobMasterLogger logger;

    public AgentJobsDispatcherService(
        JobMasterClusterConnectionConfig clusterConnectionConfig, 
        IAgentJobsDispatcherRepositoryFactory agentJobsDispatcherRepositoryFactory,
        IJobMasterRuntime jobMasterRuntime,
        IJobMasterLogger logger) : base(clusterConnectionConfig)
    {
        this.agentJobsDispatcherRepositoryFactory = agentJobsDispatcherRepositoryFactory;
        this.jobMasterRuntime = jobMasterRuntime;
        this.logger = logger;
    }

    public string AddSavePendingJob(JobRawModel jobRaw)
    {
        if (!jobRaw.AgentConnectionId.IsNotNullAndActive() || string.IsNullOrEmpty(jobRaw.BucketId))
        {
            throw new InvalidOperationException("Job is not assigned to a bucket.");
        }

        var repository = GetJobDispatcherRepository(jobRaw.AgentConnectionId!);
        var throttler = GetOperationThrottler(jobRaw.AgentConnectionId!);
        return throttler.Exec(() => repository.PushSavePendingJob(jobRaw));
    }

    public async Task<string> AddSavePendingJobAsync(JobRawModel jobRaw)
    {
        if (!jobRaw.AgentConnectionId.IsNotNullAndActive() || string.IsNullOrEmpty(jobRaw.BucketId))
        {
            throw new InvalidOperationException("Job is not assigned to a bucket.");
        }

        var repository = GetJobDispatcherRepository(jobRaw.AgentConnectionId!);
        var throttler = GetOperationThrottler(jobRaw.AgentConnectionId!);
        return await throttler.ExecAsync(() => repository.PushSavePendingJobAsync(jobRaw));
    }

    public async Task<IList<string>> BulkAddSavePendingJobAsync(List<JobRawModel> jobRawModels)
    {
        IDictionary<string, IList<JobRawModel>> jobsByBucket = new Dictionary<string, IList<JobRawModel>>();
        foreach (var jobRawModel in jobRawModels)
        {
            if (!jobRawModel.AgentConnectionId.IsNotNullAndActive() || string.IsNullOrEmpty(jobRawModel.BucketId))
            {
                throw new InvalidOperationException("Job is not assigned to a bucket.");
            }

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
            var throttler = GetOperationThrottler(agentConnectionId);
            var partitions = item.Value.Partition(JobMasterConstants.MaxBatchSizeForBulkOperation);
            
            foreach (var partition in partitions)
            {
                logger.Debug($"Bulk scheduling jobs. partition size: {partition.Count} for bucket {bucketId}", JobMasterLogSubjectType.Job, partition.First().Id);
                var partitionResult = await throttler.ExecAsync(() => repository.BulkPushSavePendingJobAsync(bucketId, partition.ToList()));
                results.AddRange(partitionResult);
            }
        }

        return results;
    }

    public string AddSavePendingRecur(RecurringScheduleRawModel recurringScheduleRaw)
    {
        if (!recurringScheduleRaw.AgentConnectionId.IsNotNullAndActive() || string.IsNullOrEmpty(recurringScheduleRaw.BucketId))
        {
            throw new InvalidOperationException("Job is not assigned to a bucket.");
        }

        var repository = GetJobDispatcherRepository(recurringScheduleRaw.AgentConnectionId!);
        var throttler = GetOperationThrottler(recurringScheduleRaw.AgentConnectionId!);
        return throttler.Exec(() => repository.PushToSaving(recurringScheduleRaw));
    }
    
    public async Task<string> AddSavePendingRecurAsync(RecurringScheduleRawModel recurringScheduleRaw)
    {
        if (!recurringScheduleRaw.AgentConnectionId.IsNotNullAndActive() || string.IsNullOrEmpty(recurringScheduleRaw.BucketId))
        {
            throw new InvalidOperationException("Job is not assigned to a bucket.");
        }
        
        var repository = GetJobDispatcherRepository(recurringScheduleRaw.AgentConnectionId!);
        var throttler = GetOperationThrottler(recurringScheduleRaw.AgentConnectionId!);
        return await throttler.ExecAsync(() => repository.PushToSavingAsync(recurringScheduleRaw));
    }

    public string AddToProcessing(string workerId, AgentConnectionId agentConnectionId, string bucketId, JobRawModel jobRaw)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        jobRaw.AssignToBucket(agentConnectionId, workerId, bucketId);
        
        var throttler = GetOperationThrottler(agentConnectionId);
        return throttler.Exec(() => repository.PushToProcessing(jobRaw));
    }

    public async Task<string> AddToProcessingAsync(string workerId, AgentConnectionId agentConnectionId, string bucketId, JobRawModel jobRaw)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        jobRaw.AssignToBucket(agentConnectionId, workerId, bucketId);
        
        var throttler = GetOperationThrottler(agentConnectionId);
        return await throttler.ExecAsync(() => repository.PushToProcessingAsync(jobRaw));
    }

    public async Task<IList<JobRawModel>> DequeueToProcessingAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs, DateTime? scheduleTo)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        var throttler = GetOperationThrottler(agentConnectionId);
        return await throttler.ExecAsync(() => repository.DequeueToProcessingAsync(bucketId, numberOfJobs, scheduleTo));
    }

    public async Task<IList<JobRawModel>> DequeueSavePendingJobsAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        var throttler = GetOperationThrottler(agentConnectionId);
        return await throttler.ExecAsync(() => repository.DequeueSavePendingJobsAsync(bucketId, numberOfJobs));
    }

    public async Task<IList<RecurringScheduleRawModel>> DequeueSavePendingRecurAsync(AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        var throttler = GetOperationThrottler(agentConnectionId);
        return await throttler.ExecAsync(() => repository.DequeueSavePendingRecurAsync(bucketId, numberOfJobs));
    }
    
    public async Task<bool> HasJobsAsync(AgentConnectionId agentConnectionId, string bucketId)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId);
        var throttler = GetOperationThrottler(agentConnectionId);
        return await throttler.ExecAsync(() => repository.HasJobsAsync(bucketId!));
    }

    public async Task CreateBucketAsync(AgentConnectionId agentConnectionId, string bucketId)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId!);
        var throttler = GetOperationThrottler(agentConnectionId!);
        await throttler.ExecAsync(() => repository.CreateBucketAsync(bucketId!));
    }

    public async Task DestroyBucketAsync(AgentConnectionId agentConnectionId, string bucketId)
    {
        var repository = GetJobDispatcherRepository(agentConnectionId!);
        var throttler = GetOperationThrottler(agentConnectionId!);
        await throttler.ExecAsync(() => repository.DestroyBucketAsync(bucketId!));
    }

    private IAgentJobsDispatcherRepository GetJobDispatcherRepository(AgentConnectionId agentConnectionId)
    {
        return agentJobsDispatcherRepositoryFactory.GetRepository(agentConnectionId);
    }
    
    private OperationThrottler GetOperationThrottler(AgentConnectionId agentConnectionId)
    {
        return jobMasterRuntime.GetOperationThrottlerForAgent(ClusterConnConfig.ClusterId, agentConnectionId.IdValue);
    }
}