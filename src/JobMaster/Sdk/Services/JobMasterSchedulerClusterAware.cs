using System;
using System.Collections.Concurrent;
using System.Threading;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils.Extensions;

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
    
    public async Task<BucketModel?> ScheduleAsync(JobRawModel rawModel)
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
            return null;
        }

        rawModel.AssignSavePendingJobToBucket(bucket);
        await agentJobsDispatcherService.AddSavePendingJobAsync(rawModel, SchedulingThrottlerFor(bucket.AgentConnectionId));
        return bucket;
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

        foreach (var bucketGroup in jobsToSave.GroupBy(j => j.BucketId!))
        {
            var bucketId = bucketGroup.Key;
            var bucketJobs = bucketGroup.ToList();
            var agentConnectionId = bucketJobs[0].AgentConnectionId!;
            var maxBatchSize = OperationThrottlerSettingsFactory.GetAgentMaxBatchSize(agentConnectionId.IdValue);
            foreach (var partition in bucketJobs.Partition(maxBatchSize))
            {
                var partitionJobs = partition.ToList();
                logger.Debug($"Bulk scheduling jobs. partition size: {partitionJobs.Count} for bucket {bucketId}",
                    JobMasterLogCategory.Job, partitionJobs.First().Id);
                await agentJobsDispatcherService.BulkAddSavePendingJobAsync(agentConnectionId, bucketId, partitionJobs,
                    SchedulingThrottlerFor(agentConnectionId));
            }
        }
    }

    public BucketModel? Schedule(RecurringScheduleRawModel rawModel)
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
            return null;
        }

        rawModel.AssignPendingRecurringScheduleToBucket(bucket);
        agentJobsDispatcherService.AddSavePendingRecur(rawModel, SchedulingThrottlerFor(bucket.AgentConnectionId));
        return bucket;
    }

    public BucketModel? Schedule(JobRawModel job)
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
            return null;
        }

        job.AssignSavePendingJobToBucket(bucket);
        agentJobsDispatcherService.AddSavePendingJob(job, SchedulingThrottlerFor(bucket.AgentConnectionId));
        return bucket;
    }

    public async Task<BucketModel?> ScheduleAsync(RecurringScheduleRawModel rawModel)
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
            return null;
        }

        rawModel.AssignPendingRecurringScheduleToBucket(bucket);
        await agentJobsDispatcherService.AddSavePendingRecurAsync(rawModel, SchedulingThrottlerFor(bucket.AgentConnectionId));
        return bucket;
    }

    public async Task ConfirmJobsOnMasterAsync(IList<JobRawModel> jobs)
    {
        var config = masterClusterConfigurationService.GetFresh();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }

        // Held on master, not PendingSave: if this insert beats the agent's save, the agent sees a duplicate and
        // drops its message, so master's assign runner (OnMaster only) must be able to pick the row up.
        foreach (var job in jobs)
        {
            job.MarkAsHeldOnMaster();
        }

        var notInserted = new List<Guid>();
        var maxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(ClusterConnConfig.ClusterId);
        foreach (var partition in jobs.Partition(maxBatchSize))
        {
            var batch = partition.ToList();
            var inserted = new HashSet<Guid>(await masterJobsService.BulkInsertIfNotExistsAsync(batch));
            notInserted.AddRange(batch.Select(j => j.Id).Where(id => !inserted.Contains(id)));
        }

        // Not inserted means already on master (e.g. the agent saved it first) or a failed row the repository
        // didn't throw for (RavenDB's per-row fallback logs and continues), so check.
        var unConfirmed = new List<Guid>();
        foreach (var partition in notInserted.Partition(maxBatchSize))
        {
            var ids = partition.ToList();
            var existing = new HashSet<Guid>(await masterJobsService.QueryExistingIdsAsync(ids));
            unConfirmed.AddRange(ids.Where(id => !existing.Contains(id)));
        }

        if (unConfirmed.Count > 0)
        {
            throw new InvalidOperationException(
                $"{unConfirmed.Count} job(s) could not be saved on master: {string.Join(", ", unConfirmed)}. Calling ConfirmOnMasterAsync again is safe.");
        }
    }

    public async Task ConfirmRecurringSchedulesOnMasterAsync(IList<RecurringScheduleRawModel> schedules)
    {
        var config = masterClusterConfigurationService.GetFresh();
        if (config?.ClusterMode == ClusterMode.Archived)
        {
            throw new InvalidOperationException("Cluster mode is archived");
        }

        foreach (var schedule in schedules)
        {
            schedule.Active();
        }

        var notInserted = new List<Guid>();
        var maxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(ClusterConnConfig.ClusterId);
        foreach (var partition in schedules.Partition(maxBatchSize))
        {
            var batch = partition.ToList();
            var inserted = new HashSet<Guid>(await masterRecurringSchedulesService.BulkInsertIfNotExistsAsync(batch));
            notInserted.AddRange(batch.Select(s => s.Id).Where(id => !inserted.Contains(id)));
        }

        // Same check as ConfirmJobsOnMasterAsync: not inserted must mean already on master.
        var unConfirmed = new List<Guid>();
        foreach (var partition in notInserted.Partition(maxBatchSize))
        {
            var ids = partition.ToList();
            var existing = new HashSet<Guid>(await masterRecurringSchedulesService.QueryExistingIdsAsync(ids));
            unConfirmed.AddRange(ids.Where(id => !existing.Contains(id)));
        }

        if (unConfirmed.Count > 0)
        {
            throw new InvalidOperationException(
                $"{unConfirmed.Count} recurring schedule(s) could not be saved on master: {string.Join(", ", unConfirmed)}. Calling ConfirmOnMasterAsync again is safe.");
        }
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

    private OperationThrottler SchedulingThrottlerFor(AgentConnectionId agentConnectionId) =>
        OperationThrottlerSettingsFactory.GetSchedulingAgentThrottler(agentConnectionId.IdValue);

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
