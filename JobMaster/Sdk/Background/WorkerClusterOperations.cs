using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Background;

internal class WorkerClusterOperations : JobMasterClusterAwareComponent, IWorkerClusterOperations
{
    private readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    private readonly IMasterJobsService masterJobsService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IMasterAgentWorkersService masterAgentWorkersService = null!;
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService = null!;
    private readonly IMasterJobExecutionService masterJobExecutionService = null!;
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
        IMasterJobExecutionService masterJobExecutionService,
        IJobMasterLogger logger) : base(clusterConnConfig)
    {
        this.agentJobsDispatcherService = agentJobsDispatcherService;
        this.masterJobsService = masterJobsService;
        this.masterDistributedLockerService = masterDistributedLockerService;
        this.masterBucketsService = masterBucketsService;
        this.masterAgentWorkersService = masterAgentWorkersService;
        this.masterRecurringSchedulesService = masterRecurringSchedulesService;
        this.masterJobExecutionService = masterJobExecutionService;
        this.logger = logger;
        this.lockKeys = new JobMasterLockKeys(clusterConnConfig.ClusterId);
    }

    public async Task AssignJobToBucketFromHeldOnMasterOrSavePendingAsync(IJobMasterBackgroundAgentWorker backgroundAgentWorker, JobRawModel jobRaw, BucketModel bucket)
    {
        if (jobRaw.Status != JobMasterJobStatus.OnMaster && 
            jobRaw.Status != JobMasterJobStatus.PendingSave)
        {
            return;
        }
        
        var originalStatus = jobRaw.Status;
        jobRaw.AssignToBucket(bucket);

        await masterJobsService.UpsertAsync(jobRaw);
        
        try
        {
            // Short-circuit: Try to inject directly into JobsExecutionEngine if on same worker
            var engine = backgroundAgentWorker.GetEngine(bucket.Id);
            if (engine != null && 
                jobRaw.IsOnBoarding() && 
                engine.OnBoardingControl.CountAvailability() > 0)
            {
                var result = await engine.TryOnBoardingJobAsync(jobRaw);
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
                
                logger.Error($"Short-circuit failed unexpected result: {result}", JobMasterLogSubjectType.Job, jobRaw.Id);
            }
            
            await agentJobsDispatcherService.AddToProcessingAsync(jobRaw);
        }
        catch (Exception ex)
        {
            logger.Error($"Failed to add job {jobRaw.Id} to processing for bucket {bucket.Id}. Reverting to {originalStatus}", JobMasterLogSubjectType.Job, jobRaw.Id, exception: ex);
            jobRaw.MarkAsHeldOnMaster();
            await masterJobsService.UpsertAsync(jobRaw);
            throw;
        }
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

    public void Upsert(JobRawModel jobRawModel, JobExecution? jobExecution = null)
    {
        masterJobsService.Upsert(jobRawModel);
        
        if (jobExecution != null)
        {
            masterJobExecutionService.Save(jobExecution);
        }
    }
    
    public async Task UpsertAsync(JobRawModel jobRawModel, JobExecution? jobExecution = null)
    {
        await masterJobsService.UpsertAsync(jobRawModel);
        
        if (jobExecution != null)
        {
            await masterJobExecutionService.SaveAsync(jobExecution);
        }
    }

    public Task SaveJobExecutionAsync(JobExecution jobExecution)
    {
        return masterJobExecutionService.SaveAsync(jobExecution);
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
    
    public async Task<int> CountActiveCoordinatorWorkersAsync()
    {
        var workers = await masterAgentWorkersService.QueryWorkersAsync();
        return workers.Count(x => x.Status() == AgentWorkerStatus.Active && 
                                  (x.Mode == AgentWorkerMode.Coordinator || x.Mode == AgentWorkerMode.Full));
    }
    
    public async Task<int> CountWorkersAsync()
    {
        var workers = await masterAgentWorkersService.QueryWorkersAsync();
        return workers.Count(x => x.Status() == AgentWorkerStatus.Active);
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
                if (e is JobMasterDuplicationException)
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
                if (e is JobMasterDuplicationException)
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
