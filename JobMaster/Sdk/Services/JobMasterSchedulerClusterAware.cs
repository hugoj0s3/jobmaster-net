using System;
using System.Collections.Concurrent;
using System.Threading;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services;

public class JobMasterSchedulerClusterAware : JobMasterClusterAwareComponent, IJobMasterSchedulerClusterAware   
{
    private IAgentJobsDispatcherService agentJobsDispatcherService = null!;
    private IMasterBucketsService masterBucketsService = null!;
    private IMasterJobsService masterJobsService = null!;
    private IMasterRecurringSchedulesService masterRecurringSchedulesService = null!;
    private IMasterDistributedLockerService masterDistributedLockerService = null!;
    private IMasterClusterConfigurationService masterClusterConfigurationService = null!;
    private readonly IJobMasterLogger logger;

    private JobMasterLockKeys lockKeys = null!;

    public JobMasterSchedulerClusterAware(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IAgentJobsDispatcherService agentJobsDispatcherService,
        IMasterBucketsService masterBucketsService,
        IMasterJobsService masterJobsService,
        IMasterRecurringSchedulesService masterRecurringSchedulesService,
        IMasterDistributedLockerService masterDistributedLockerService,
        IMasterClusterConfigurationService masterClusterConfigurationService,
        IJobMasterLogger logger) : base(clusterConnConfig)
    {
        this.agentJobsDispatcherService = agentJobsDispatcherService;
        this.masterBucketsService = masterBucketsService;
        this.masterJobsService = masterJobsService;
        this.masterRecurringSchedulesService = masterRecurringSchedulesService;
        this.masterDistributedLockerService = masterDistributedLockerService;
        this.masterClusterConfigurationService = masterClusterConfigurationService;
        this.logger = logger;

        lockKeys = new JobMasterLockKeys(clusterConnConfig.ClusterId);
    }
    
    public async Task ScheduleAsync(JobRawModel rawModel)
    {
        var config = masterClusterConfigurationService.Get();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }
        
        var bucket = await GetBucketAvailableForJobAsync(rawModel);
        
        if (config?.ClusterMode == ClusterMode.Passive || bucket == null || string.IsNullOrEmpty(bucket.AgentWorkerId))
        {
            EnforceMasterStoreSizeLimit(rawModel);
            rawModel.MarkAsHeldOnMaster();
            await masterJobsService.UpsertAsync(rawModel);
            return;
        }

        rawModel.AssignSavePendingJobToBucket(bucket.AgentConnectionId, bucket.AgentWorkerId!, bucket.Id);
        await agentJobsDispatcherService.AddSavePendingJobAsync(rawModel);
    }
    
    public async Task BulkScheduleAsync(List<JobRawModel> rawModels)
    {
        var config = masterClusterConfigurationService.Get();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }
        
        var jobsToSave = new List<JobRawModel>();
        // Assign to bucket first.
        foreach (var jobRawModel in rawModels)
        {
            var bucket = await GetBucketAvailableForJobAsync(jobRawModel);
            if (config?.ClusterMode == ClusterMode.Passive || bucket == null || string.IsNullOrEmpty(bucket.AgentWorkerId))
            {
                EnforceMasterStoreSizeLimit(jobRawModel);
                jobRawModel.MarkAsHeldOnMaster();
                await masterJobsService.UpsertAsync(jobRawModel);
                continue;
            }

            jobRawModel.AssignSavePendingJobToBucket(bucket.AgentConnectionId, bucket.AgentWorkerId!, bucket.Id);
            jobsToSave.Add(jobRawModel);
        }

        if (jobsToSave.Count > 0)
        {
            logger.Debug($"$Bulk scheduling jobs. {jobsToSave.Count}", JobMasterLogSubjectType.Job, jobsToSave.First().Id);
            await agentJobsDispatcherService.BulkAddSavePendingJobAsync(jobsToSave);
        }
    }

    public void Schedule(RecurringScheduleRawModel rawModel)
    {
        var config = masterClusterConfigurationService.Get();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }

        var bucket = GetBucketAvailableForJob(rawModel);
        
        if (config?.ClusterMode == ClusterMode.Passive || bucket == null || string.IsNullOrEmpty(bucket.AgentWorkerId))
        {
            EnforceMasterStoreSizeLimit(rawModel);
            rawModel.Active(); 
            masterRecurringSchedulesService.Upsert(rawModel);
            return;
        }

        rawModel.AssignPendingRecurringScheduleToBucket(bucket.AgentConnectionId, bucket.AgentWorkerId!, bucket.Id); 
        agentJobsDispatcherService.AddSavePendingRecur(rawModel);
    }

    public void Schedule(JobRawModel job)
    {
        var config = masterClusterConfigurationService.Get();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }

        var bucket = GetBucketAvailableForJob(job);
        
        if (config?.ClusterMode == ClusterMode.Passive || bucket == null || string.IsNullOrEmpty(bucket.AgentWorkerId))
        {
            EnforceMasterStoreSizeLimit(job);
            job.MarkAsHeldOnMaster();
            masterJobsService.Upsert(job);
            return;
        }

        job.AssignSavePendingJobToBucket(bucket.AgentConnectionId, bucket.AgentWorkerId!, bucket.Id);
        agentJobsDispatcherService.AddSavePendingJob(job);
    }

    public async Task ScheduleAsync(RecurringScheduleRawModel rawModel)
    {
        var config = masterClusterConfigurationService.Get();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }
        
        var bucket = await GetBucketAvailableForJobAsync(rawModel);
        if (config?.ClusterMode == ClusterMode.Passive || bucket == null || string.IsNullOrEmpty(bucket.AgentWorkerId))
        {
            EnforceMasterStoreSizeLimit(rawModel);
            rawModel.Active();
            await masterRecurringSchedulesService.UpsertAsync(rawModel);
            return;
        }

        rawModel.AssignPendingRecurringScheduleToBucket(bucket.AgentConnectionId, bucket.AgentWorkerId!, bucket.Id);
        await agentJobsDispatcherService.AddSavePendingRecurAsync(rawModel);
    }

    public async Task<bool> CancelJobAsync(Guid jobId)
    {
        var jobEntity = await masterJobsService.GetAsync(jobId);
        if (jobEntity == null)
        {
            return false;
        }

        if (!jobEntity.TryToCancel())
        {
            return false;
        }

        await masterJobsService.UpsertAsync(jobEntity);
        return true;
    }

    public bool CancelJob(Guid jobId)
    {
        var jobEntity = masterJobsService.Get(jobId);
        if (jobEntity == null)
        {
            return false;
        }

        if (!jobEntity.TryToCancel())
        {
            return false;
        }

        masterJobsService.Upsert(jobEntity);
        return true;
    }

    public async Task<bool> CancelRecurringAsync(Guid id)
    {
        var recurringScheduleEntity = await masterRecurringSchedulesService.GetAsync(id);
        if (recurringScheduleEntity == null)
        {
            return false;
        }

        if (!recurringScheduleEntity.TryToCancel())
        {
            return false;
        }
        
        await masterRecurringSchedulesService.UpsertAsync(recurringScheduleEntity);
        return true;
    }

    public bool CancelRecurring(Guid id)
    {
        var recurringScheduleEntity = masterRecurringSchedulesService.Get(id);
        if (recurringScheduleEntity == null)
        {
            return false;
        }

        if (!recurringScheduleEntity.TryToCancel())
        {
            return false;
        }
        
        masterRecurringSchedulesService.Upsert(recurringScheduleEntity);
        return true;
    }

    public async Task<bool> ReScheduleAsync(Guid jobId, DateTime scheduledAt)
    {
        var config = masterClusterConfigurationService.Get();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }
        
        var jobEntity = await masterJobsService.GetAsync(jobId);
        if (jobEntity == null)
        {
            return false;
        }
        
        if (!jobEntity.CanReSchedule())
        {
            return false;
        }

        jobEntity.ReSchedule(scheduledAt);
        await masterJobsService.UpsertAsync(jobEntity);
        
        return true;
    }

    public bool ReSchedule(Guid jobId, DateTime scheduledAt)
    {
        var config = masterClusterConfigurationService.Get();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }
        
        var jobEntity =  masterJobsService.Get(jobId);
        if (jobEntity == null)
        {
            return false;
        }
        
        if (!jobEntity.CanReSchedule())
        {
            return false;
        }

        jobEntity.ReSchedule(scheduledAt);
        masterJobsService.Upsert(jobEntity);
        
        return true;
    }

    private void EnforceMasterStoreSizeLimit(JobRawModel job)
    {
        int correlationLen = 32;
        var estimated = JobMasterRawMessage.CalcEstimateByteSize(job, clusterIdLength: ClusterConnConfig.ClusterId.Length, correlationIdLength: correlationLen);
        var max = masterClusterConfigurationService.Get()?.MaxMessageByteSize ?? new ClusterConfigurationModel(ClusterConnConfig.ClusterId).MaxMessageByteSize;
        if (estimated > max)
        {
            throw new ArgumentException($"Message size {estimated} exceeds maximum allowed size {max}.");
        }
    }

    private void EnforceMasterStoreSizeLimit(RecurringScheduleRawModel recur)
    {
        int correlationLen = 32;
        var estimated = JobMasterRawMessage.CalcEstimateByteSize(recur, clusterIdLength: ClusterConnConfig.ClusterId.Length, correlationIdLength: correlationLen);
        var max = masterClusterConfigurationService.Get()?.MaxMessageByteSize ?? new ClusterConfigurationModel(ClusterConnConfig.ClusterId).MaxMessageByteSize;
        if (estimated > max)
        {
            throw new ArgumentException($"Message size {estimated} exceeds maximum allowed size {max}.");
        }
    }
    
    private BucketModel? GetBucketAvailableForJob(JobRawModel job)
    {
        return masterBucketsService.SelectBucket(JobMasterConstants.BucketDefaultAllowDiscrepancy, job.Priority, job.WorkerLane);
    }

    private  BucketModel? GetBucketAvailableForJob(RecurringScheduleRawModel recurringSchedule)
    {
        return masterBucketsService.SelectBucket(JobMasterConstants.BucketDefaultAllowDiscrepancy, recurringSchedule.Priority ?? JobMasterPriority.Medium);
    }
    
    private async Task< BucketModel?> GetBucketAvailableForJobAsync(JobRawModel job)
    {
        return await masterBucketsService.SelectBucketAsync(JobMasterConstants.BucketDefaultAllowDiscrepancy, job.Priority, job.WorkerLane);
    }

    private async Task< BucketModel?> GetBucketAvailableForJobAsync(RecurringScheduleRawModel recurringSchedule)
    {
        return await masterBucketsService.SelectBucketAsync(JobMasterConstants.BucketDefaultAllowDiscrepancy, recurringSchedule.Priority, recurringSchedule.WorkerLane);
    }
}