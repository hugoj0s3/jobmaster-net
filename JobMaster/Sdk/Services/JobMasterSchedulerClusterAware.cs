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

internal class JobMasterSchedulerClusterAware : JobMasterClusterAwareComponent, IJobMasterSchedulerClusterAware   
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
    
    public async Task ScheduleAsync(JobRawModel rawModel, bool notifyAgent = true)
    {
        var config = masterClusterConfigurationService.Get();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }

        EnforceSizeLimit(rawModel);

        var bucket = await GetBucketAvailableForJobAsync(rawModel);

        if (config?.ClusterMode == ClusterMode.Migrating || bucket == null || string.IsNullOrEmpty(bucket.AgentWorkerId))
        {
            rawModel.MarkAsHeldOnMaster();
            await masterJobsService.AddAsync(rawModel);
            return;
        }

        rawModel.AssignSavePendingJobToBucket(bucket);
        await agentJobsDispatcherService.AddSavePendingJobAsync(rawModel, notifyAgent);
    }

    public async Task BulkScheduleAsync(List<JobRawModel> rawModels)
    {
        var config = masterClusterConfigurationService.Get();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }

        // Validate the whole batch up front — no side effects yet — so one oversized job
        // fails the entire call before anything is written or dispatched.
        foreach (var jobRawModel in rawModels)
        {
            EnforceSizeLimit(jobRawModel);
        }

        var jobsToSave = new List<JobRawModel>();
        // Assign to bucket first.
        foreach (var jobRawModel in rawModels)
        {
            var bucket = await GetBucketAvailableForJobAsync(jobRawModel);
            if (config?.ClusterMode == ClusterMode.Migrating || bucket == null || string.IsNullOrEmpty(bucket.AgentWorkerId))
            {
                jobRawModel.MarkAsHeldOnMaster();
                await masterJobsService.AddAsync(jobRawModel);
                continue;
            }

            jobRawModel.AssignSavePendingJobToBucket(bucket);
            jobsToSave.Add(jobRawModel);
        }

        if (jobsToSave.Count > 0)
        {
            logger.Debug($"$Bulk scheduling jobs. {jobsToSave.Count}", JobMasterLogCategory.Job, jobsToSave.First().Id);
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

        EnforceSizeLimit(rawModel);

        var bucket = GetBucketAvailableForJob(rawModel);

        if (config?.ClusterMode == ClusterMode.Migrating || bucket == null || string.IsNullOrEmpty(bucket.AgentWorkerId))
        {
            rawModel.Active();
            masterRecurringSchedulesService.Add(rawModel);
            return;
        }

        rawModel.AssignPendingRecurringScheduleToBucket(bucket);
        agentJobsDispatcherService.AddSavePendingRecur(rawModel);
    }

    public void Schedule(JobRawModel job, bool notifyAgent = true)
    {
        var config = masterClusterConfigurationService.Get();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }

        EnforceSizeLimit(job);

        var bucket = GetBucketAvailableForJob(job);

        if (config?.ClusterMode == ClusterMode.Migrating || bucket == null || string.IsNullOrEmpty(bucket.AgentWorkerId))
        {
            job.MarkAsHeldOnMaster();
            masterJobsService.Add(job);
            return;
        }

        job.AssignSavePendingJobToBucket(bucket);
        agentJobsDispatcherService.AddSavePendingJob(job, notifyAgent);
    }

    public async Task ScheduleAsync(RecurringScheduleRawModel rawModel)
    {
        var config = masterClusterConfigurationService.Get();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }

        EnforceSizeLimit(rawModel);

        var bucket = await GetBucketAvailableForJobAsync(rawModel);
        if (config?.ClusterMode == ClusterMode.Migrating || bucket == null || string.IsNullOrEmpty(bucket.AgentWorkerId))
        {
            rawModel.Active();
            await masterRecurringSchedulesService.AddAsync(rawModel);
            return;
        }

        rawModel.AssignPendingRecurringScheduleToBucket(bucket);
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

        await masterJobsService.UpdateAsync(jobEntity);
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

        masterJobsService.UpdateAsync(jobEntity);
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
        
        await masterRecurringSchedulesService.UpdateAsync(recurringScheduleEntity);
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
        
        masterRecurringSchedulesService.Update(recurringScheduleEntity);
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
        await masterJobsService.UpdateAsync(jobEntity);
        
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
        masterJobsService.Update(jobEntity);
        
        return true;
    }

    private void EnforceSizeLimit(JobRawModel job)
    {
        var estimated = JobMasterRawMessage.CalcEstimateByteSize(
            job.MsgData, 
            clusterIdLength: ClusterConnConfig.ClusterId.Length, 
            metadata: job.Metadata);
        
        var max = masterClusterConfigurationService.Get()?.MaxMessageByteSize ?? 
                  new ClusterConfigurationModel(ClusterConnConfig.ClusterId).MaxMessageByteSize;
        if (estimated > max)
        {
            throw new ArgumentException($"Message size {estimated} exceeds maximum allowed size {max}.");
        }
    }

    private void EnforceSizeLimit(RecurringScheduleRawModel recur)
    {
        var estimated = JobMasterRawMessage
            .CalcEstimateByteSize(
                recur.MsgData, 
                clusterIdLength: ClusterConnConfig.ClusterId.Length, 
                metadata: recur.Metadata);
        
        var max = masterClusterConfigurationService.Get()?.MaxMessageByteSize ?? 
                  new ClusterConfigurationModel(ClusterConnConfig.ClusterId).MaxMessageByteSize;
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