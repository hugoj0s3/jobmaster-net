using JobMaster.Contracts.Extensions;
using JobMaster.Contracts.Models;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Exceptions;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Services.Agent;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Background;

public class WorkerClusterOperations : JobMasterClusterAwareComponent, IWorkerClusterOperations
{
    private readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    private readonly IMasterJobsService masterJobsService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IMasterAgentWorkersService masterAgentWorkersService = null!;
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService = null!;
    private readonly IJobMasterLogger logger = null!;
    private readonly JobMasterLockKeys lockKeys = null!;

    public WorkerClusterOperations(
        JobMasterClusterConnectionConfig clusterConnConfig, 
        IAgentJobsDispatcherService agentJobsDispatcherService, 
        IMasterJobsService masterJobsService, 
        IMasterDistributedLockerService masterDistributedLockerService, 
        IMasterBucketsService masterBucketsService, 
        IMasterAgentWorkersService masterAgentWorkersService, 
        IMasterRecurringSchedulesService masterRecurringSchedulesService, 
        IJobMasterLogger logger) : base(clusterConnConfig)
    {
        this.agentJobsDispatcherService = agentJobsDispatcherService;
        this.masterJobsService = masterJobsService;
        this.masterDistributedLockerService = masterDistributedLockerService;
        this.masterBucketsService = masterBucketsService;
        this.masterAgentWorkersService = masterAgentWorkersService;
        this.masterRecurringSchedulesService = masterRecurringSchedulesService;
        this.logger = logger;
        this.lockKeys = new JobMasterLockKeys(clusterConnConfig.ClusterId);
    }

    public async Task AssignJobToBucketFromHeldOnMasterOrSavePendingAsync(IJobMasterBackgroundAgentWorker backgroundAgentWorker, JobRawModel jobRaw, BucketModel bucket)
    {
        if (jobRaw.Status != JobMasterJobStatus.HeldOnMaster && 
            jobRaw.Status != JobMasterJobStatus.SavePending)
        {
            return;
        }

        var agentWorkerId = bucket.AgentWorkerId.NotNull();
        jobRaw.AssignToBucket(bucket.AgentConnectionId, agentWorkerId, bucket.Id);

        await masterJobsService.UpsertAsync(jobRaw);
        
        // Short-circuit: Try to inject directly into JobsExecutionEngine if on same worker
        var engine = backgroundAgentWorker.SafeGetOrCreateEngine(bucket.Priority, bucket.Id);
        if (engine != null && jobRaw.IsOnBoarding() && engine.OnBoardingControl.CountAvailability() > 0)
        {
            var result = await engine.TryOnBoardingJobAsync(jobRaw, forceIfNoCapacity: true);
            if (result == OnBoardingResult.Accepted)
            {
                logger.Debug($"Short-circuit: Injecting job {jobRaw.Id} directly into engine for bucket {bucket.Id}", JobMasterLogSubjectType.Job, jobRaw.Id);
                return;
            }
            
            if (result == OnBoardingResult.MovedToMaster)
            {
                logger.Warn($"Short-cut failed moved to master", JobMasterLogSubjectType.Job, jobRaw.Id);
                return;
            }
        }
        
        await agentJobsDispatcherService.AddToProcessingAsync(agentWorkerId, bucket.AgentConnectionId, bucket.Id, jobRaw);
    }

    public void MarkAsHeldOnMaster(Guid jobId)
    {
        var job = masterJobsService.Get(jobId);
        if (job is null)
        {
            return;
        }

        job.MarkAsHeldOnMaster();
        masterJobsService.Upsert(job);
    }

    public void CancelJob(Guid jobId)
    {
        var job = masterJobsService.Get(jobId);

        if (job?.TryToCancel() == true)
        {
            masterJobsService.Upsert(job);
        }
    }

    public void Upsert(JobRawModel jobRawModel)
    {
        masterJobsService.Upsert(jobRawModel);
    }
    
    public async Task UpsertAsync(JobRawModel jobRawModel)
    {
        await masterJobsService.UpsertAsync(jobRawModel);
    }

    public void Upsert(RecurringScheduleRawModel jobRawModel)
    {
        masterRecurringSchedulesService.Upsert(jobRawModel);
    }

    public async Task MarkBucketAsLostAsync(BucketModel bucket)
    {
        var lockToken = this.masterDistributedLockerService.TryLock(lockKeys.BucketLock(bucket.Id), TimeSpan.FromSeconds(10));
        if (lockToken == null)
        {
            return;
        }
            
        bucket.MarkAsLost();
        await masterBucketsService.UpdateAsync(bucket);
            
        this.masterDistributedLockerService.ReleaseLock(lockKeys.BucketLock(bucket.Id), lockToken);
    }

    public async Task MarkBucketAsLostAsync(string bucketId)
    {
        var bucket = masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket is null)
        {
            return;
        }
        
        await MarkBucketAsLostAsync(bucket);
    }

    public async Task MarkBucketAsLostIfNotDrainingAsync(string bucketId)
    {
       var bucket = masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
       if (bucket is null)
       {
           return;
       }

       if (bucket.Status == BucketStatus.Draining || bucket.Status == BucketStatus.ReadyToDrain || bucket.Status == BucketStatus.Lost)
       {
           return;
       }
       
       await MarkBucketAsLostAsync(bucket);
    }
    
    public void MarkBucketAsLost(BucketModel bucket)
    {
        var lockToken = this.masterDistributedLockerService.TryLock(lockKeys.MarkBucketAsLostLock(bucket.Id), TimeSpan.FromSeconds(10));
        if (lockToken == null)
        {
            return;
        }
            
        bucket.MarkAsLost();
        masterBucketsService.Update(bucket);
            
        this.masterDistributedLockerService.ReleaseLock(lockKeys.MarkBucketAsLostLock(bucket.Id), lockToken);
    }
    
    public async Task<int> CountAliveWorkersAsync()
    {
        var workers = await masterAgentWorkersService.GetWorkersAsync();
        return workers.Count(x => x.IsAlive);
    }
    
    public void CancelRecurringSchedule(Guid id)
    {
        var recurringScheduleRawModel = masterRecurringSchedulesService.Get(id);

        if (recurringScheduleRawModel?.TryToCancel() == true)
        {
            masterRecurringSchedulesService.Upsert(recurringScheduleRawModel);
        }
    }

    public async Task ExecWithRetryAsync(Action<IWorkerClusterOperations> func, int maxRetries = 5, int millisecondsToDelay = 200)
    {
        if (func is null) throw new ArgumentNullException(nameof(func));
        if (maxRetries < 1) throw new ArgumentOutOfRangeException(nameof(maxRetries), maxRetries, "maxRetries must be >= 1");

        var attempt = 1;
        while (true)
        {
            try
            {
                func.Invoke(this);
                return;
            }
            catch (Exception e)
            {
                if (e is JobDuplicationException)
                {
                    throw;
                }
                
                logger.Error("Failed to execute function", exception: e);
                if (attempt >= maxRetries)
                {
                    throw;
                }

                attempt++;

                if (JobMasterRandomUtil.GetBoolean(0.25))
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(millisecondsToDelay));
                }

                var jitter = CalcJitterOnExecWithRetry(attempt);
                await Task.Delay(TimeSpan.FromMilliseconds(jitter));
            }
        }
    }

    public async Task ExecWithRetryAsync(Func<IWorkerClusterOperations, Task> func, int maxRetries = 5, int millisecondsToDelay = 200)
    {
        if (func is null) throw new ArgumentNullException(nameof(func));
        if (maxRetries < 1) throw new ArgumentOutOfRangeException(nameof(maxRetries), maxRetries, "maxRetries must be >= 1");

        var attempt = 1;
        while (true)
        {
            try
            {
                await func.Invoke(this);
                return;
            }
            catch (Exception e)
            {
                if (e is JobDuplicationException)
                {
                    throw;
                }
                
                logger.Error("Failed to execute function", exception: e);
                if (attempt >= maxRetries)
                {
                    throw;
                }
                
                attempt++;

                if (JobMasterRandomUtil.GetBoolean(0.25))
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(millisecondsToDelay));
                }
                
                var jitter = CalcJitterOnExecWithRetry(attempt);
                await Task.Delay(TimeSpan.FromMilliseconds(jitter));
            }
        }
    }

    public async Task AddAsync(JobRawModel job)
    {
        await masterJobsService.AddAsync(job);
    }

    public void Insert(JobRawModel job)
    {
        masterJobsService.Add(job);
    }

    private static int CalcJitterOnExecWithRetry(int attempt)
    {
        var minJitter = 5 * attempt;
        var maxJitter = minJitter + 100;
        var jitter = JobMasterRandomUtil.GetInt(minJitter, maxJitter);
        return jitter;
    }
}
