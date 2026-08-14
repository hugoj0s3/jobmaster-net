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

    // Same-worker short-circuit still happens per job (cheap, in-memory), but everything that needs
    // an actual agent-DB write is collected and sent through BulkAddForProcessingAsync in one call
    // instead of one dispatch call per job. Caller passes one bucket's jobs at a time -- grouping by
    // bucket and partitioning happen in AssignJobsToBucketsRunner, so a failure here only holds back
    // this one partition instead of the whole tick's batch.
    public async Task BulkDispatchJobsToBucketAsync(IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        BucketModel bucket, IList<JobRawModel> jobs)
    {
        var toBulkDispatch = new List<JobRawModel>();

        foreach (var jobRaw in jobs)
        {
            if (jobRaw.Status != JobMasterJobStatus.InBucket)
            {
                // Skip, don't throw: this job is one of a whole partition batched into this call,
                // and its bad status isn't the other jobs' problem -- throwing here would abort
                // dispatch for the rest of a perfectly fine partition. Left untouched (not held on
                // master) since we don't know what state it's legitimately in.
                logger.Error($"Job {jobRaw.Id} is not in bucket, skipping dispatch. Status: {jobRaw.Status}", JobMasterLogCategory.Job, jobRaw.Id);
                continue;
            }

            try
            {
                var engine = backgroundAgentWorker.GetEngine(bucket.Id);
                if (engine != null &&
                    jobRaw.IsWithinOnboardingWindow() &&
                    engine.HasOnBoardingAvailability())
                {
                    var result = await engine.TryOnBoardingJobAsync(jobRaw, forceIfNoCapacity: true);
                    if (result == OnBoardingResult.Accepted)
                    {
                        logger.Debug($"Short-circuit: Injecting job {jobRaw.Id} directly into engine for bucket {bucket.Id}", JobMasterLogCategory.Job, jobRaw.Id);
                        continue;
                    }

                    if (result == OnBoardingResult.MovedToMaster)
                    {
                        logger.Warn($"Short-circuit failed, moved to master. JobId={jobRaw.Id}", JobMasterLogCategory.Job, jobRaw.Id);
                        continue;
                    }

                    if (result == OnBoardingResult.Cancelled)
                    {
                        logger.Warn($"Short-circuit failed: job or recurring schedule was cancelled. JobId={jobRaw.Id}", JobMasterLogCategory.Job, jobRaw.Id);
                        continue;
                    }

                    logger.Error($"Short-circuit failed unexpected result: {result}", JobMasterLogCategory.Job, jobRaw.Id);
                    jobRaw.MarkAsHeldOnMaster();
                    await masterJobsService.UpdateAsync(jobRaw);
                    continue;
                }

                toBulkDispatch.Add(jobRaw);
            }
            catch (Exception ex)
            {
                logger.Error($"Failed short-circuit attempt for job {jobRaw.Id}, holding on master.", JobMasterLogCategory.Job, jobRaw.Id, exception: ex);
                jobRaw.MarkAsHeldOnMaster();
                await masterJobsService.UpdateAsync(jobRaw);
            }
        }

        if (toBulkDispatch.Count == 0)
        {
            return;
        }

        try
        {
            await agentJobsDispatcherService.BulkAddForProcessingAsync(bucket.AgentConnectionId, bucket.Id, toBulkDispatch);
        }
        catch (Exception ex)
        {
            logger.Error($"Failed to bulk dispatch {toBulkDispatch.Count} jobs to bucket {bucket.Id}.", JobMasterLogCategory.Job, toBulkDispatch[0].Id, exception: ex);
            var idsToHeldOnMaster = toBulkDispatch.Select(x => x.Id).ToList();
            await masterJobsService.BulkUpdateAsync(BulkJobUpdateRequest.HeldOnMaster(idsToHeldOnMaster), useAcquireThrottler: true);
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
        masterJobsService.Update(job);
    }

    public void CancelJob(Guid jobId)
    {
        var job = masterJobsService.Get(jobId);

        if (job?.TryToCancel() == true)
        {
            masterJobsService.Update(job);
        }
    }

    public void Upsert(JobRawModel jobRawModel)
    {
        if (jobRawModel.Version is null)
        {
            this.masterJobsService.Add(jobRawModel);
            return;
        }
        
        if (ExistsJob(jobRawModel.Id))
        {
            this.masterJobsService.Update(jobRawModel);
            return;
        }

        try
        {
            this.masterJobsService.Add(jobRawModel);
        }
        catch (JobMasterDuplicationException e)
        {
            logger.Error($"Job was added by another process or thread. JobId={jobRawModel.Id}", JobMasterLogCategory.Job, jobRawModel.Id, exception: e);
        }
    }

    public async Task UpsertAsync(JobRawModel jobRawModel)
    {
        if (jobRawModel.Version is null)
        {
            await masterJobsService.AddAsync(jobRawModel);
            return;
        }

        if (await ExistsJobAsync(jobRawModel.Id))
        {
            await this.masterJobsService.UpdateAsync(jobRawModel);
            return;
        }
        
        try
        {
            await this.masterJobsService.AddAsync(jobRawModel);
        }
        catch (JobMasterDuplicationException e)
        {
            logger.Error($"Job was added by another process or thread. JobId={jobRawModel.Id}", JobMasterLogCategory.Job, jobRawModel.Id, exception: e);
        }
    }
    public void Update(JobRawModel jobRawModel, JobExecution? jobExecution = null)
    {
        masterJobsService.Update(jobRawModel, jobExecution);
    }

    public async Task UpdateAsync(JobRawModel jobRawModel, JobExecution? jobExecution = null)
    {
        await masterJobsService.UpdateAsync(jobRawModel, jobExecution);
    }

    public Task AddJobExecutionAsync(JobExecution jobExecution)
    {
        return masterJobsService.AddJobExecutionAsync(jobExecution);
    }

    public void Upsert(RecurringScheduleRawModel recurringScheduleRawModel)
    {
        if (recurringScheduleRawModel.Version is null)
        {
            this.masterRecurringSchedulesService.Add(recurringScheduleRawModel);
            return;
        }
        
        if (ExistsRecurringSchedule(recurringScheduleRawModel.Id))
        {
            this.masterRecurringSchedulesService.Update(recurringScheduleRawModel);
            return;
        }

        try
        {
            this.masterRecurringSchedulesService.Add(recurringScheduleRawModel);
        }
        catch (JobMasterDuplicationException e)
        {
            logger.Error($"Recurring schedule was added by another process or thread. Id={recurringScheduleRawModel.Id}", JobMasterLogCategory.RecurringSchedule, recurringScheduleRawModel.Id, exception: e);
        }
    }

    public async Task MarkBucketAsLostAsync(BucketModel bucket)
    {
        var lockToken = this.masterDistributedLockerService.TryLock(lockKeys.BucketLock(bucket.Id), TimeSpan.FromSeconds(10));
        if (lockToken == null)
        {
            logger.Debug($"MarkBucketAsLostAsync: failed to acquire lock for bucket {bucket.Id}", JobMasterLogCategory.Bucket, bucket.Id);
            return;
        }

        try
        {
            bucket.MarkAsLost();
            await masterBucketsService.UpdateAsync(bucket);
        }
        finally
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.BucketLock(bucket.Id), lockToken);
        }
    }

    public async Task MarkBucketAsLostAsync(string bucketId)
    {
        // Fresh, uncached read -- not masterBucketsService.Get(bucketId, allowedDiscrepancy), which
        // can silently return a stale cached snapshot never invalidated for this specific bucket,
        // causing MarkAsLost() to be applied to (and re-written from) stale data or skipped
        // entirely if the cache is out of sync. Same rationale as MarkBucketAsLostIfNotDrainingAsync
        // below.
        var bucket = await masterBucketsService.GetNoCacheAsync(bucketId);
        if (bucket is null)
        {
            return;
        }

        await MarkBucketAsLostAsync(bucket);
    }

    public async Task MarkBucketAsLostIfNotDrainingAsync(string bucketId)
    {
       var bucket = masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
       if (bucket is null || IsExemptFromLostMarking(bucket.Status))
       {
           return;
       }

       var lockToken = masterDistributedLockerService.TryLock(lockKeys.BucketLock(bucketId), TimeSpan.FromSeconds(10));
       if (lockToken == null)
       {
           return;
       }

       try
       {
           // Re-check under the lock with an uncached read: the bucket may have moved into an
           // exempt status (e.g. picked up for draining, or already finished draining) since the
           // check above, and the cache could still be serving that now-stale snapshot.
           bucket = await masterBucketsService.GetNoCacheAsync(bucketId);
           if (bucket is null || IsExemptFromLostMarking(bucket.Status))
           {
               return;
           }

           bucket.MarkAsLost();
           await masterBucketsService.UpdateAsync(bucket);
       }
       finally
       {
           masterDistributedLockerService.ReleaseLock(lockKeys.BucketLock(bucketId), lockToken);
       }
    }

    private static bool IsExemptFromLostMarking(BucketStatus status) =>
        status is BucketStatus.Draining or BucketStatus.ReadyToDrain or BucketStatus.Lost or BucketStatus.ReadyToDelete;

    public async Task MarkBucketAsReadyToDeleteAsync(string bucketId)
    {
        var bucket = masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket is null)
        {
            return;
        }

        bucket.ReadyToDelete();
        await masterBucketsService.UpdateAsync(bucket);
    }

    public async Task MarkBucketAsReadyToDrainAsync(string bucketId)
    {
        var bucket = masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket is null)
        {
            return;
        }

        if (!bucket.ReadyToDrainFromCompleting())
        {
            logger.Debug($"MarkBucketAsReadyToDrainAsync: skipped — bucket {bucketId} is not in a valid state for this transition. Status={bucket.Status}", JobMasterLogCategory.Bucket, bucketId);
            return;
        }

        await masterBucketsService.UpdateAsync(bucket);
    }

    public void MarkBucketAsLost(BucketModel bucket)
    {
        var lockToken = this.masterDistributedLockerService.TryLock(lockKeys.BucketLock(bucket.Id), TimeSpan.FromSeconds(10));
        if (lockToken == null)
        {
            return;
        }

        try
        {
            bucket.MarkAsLost();
            masterBucketsService.Update(bucket);
        }
        finally
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.BucketLock(bucket.Id), lockToken);
        }
    }
    
    public async Task<int> CountActiveCoordinatorWorkersAsync()
    {
        var workers = await masterAgentWorkersService.QueryWorkersAsync();
        return workers.Count(x => x.Status() == AgentWorkerStatus.Active && x.Mode.IsFullOr(AgentWorkerMode.Coordinator));
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
            masterRecurringSchedulesService.Update(recurringScheduleRawModel);
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
                if (e is JobMasterDuplicationException or JobMasterVersionConflictException)
                {
                    throw;
                }

                logger.Error($"ExecWithRetry failed on attempt {attempt}/{maxRetries}.", exception: e);
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
                if (e is JobMasterDuplicationException or JobMasterVersionConflictException)
                {
                    throw;
                }

                logger.Error($"ExecWithRetry failed on attempt {attempt}/{maxRetries}.", exception: e);
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

    public async Task<T> ExecWithRetryAsync<T>(Func<IWorkerClusterOperations, Task<T>> func, int maxRetries = 5, int millisecondsToDelay = 200)
    {
        if (func is null) throw new ArgumentNullException(nameof(func));
        if (maxRetries < 1) throw new ArgumentOutOfRangeException(nameof(maxRetries), maxRetries, "maxRetries must be >= 1");

        var attempt = 1;
        while (true)
        {
            try
            {
                return await func(this);
            }
            catch (Exception e)
            {
                if (e is JobMasterDuplicationException or JobMasterVersionConflictException) throw;

                logger.Error($"ExecWithRetry failed on attempt {attempt}/{maxRetries}.", exception: e);
                if (attempt >= maxRetries) throw;

                attempt++;
                if (JobMasterRandomUtil.GetBoolean(0.25))
                    await Task.Delay(TimeSpan.FromMilliseconds(millisecondsToDelay));

                var jitter = CalcJitterOnExecWithRetry(attempt);
                await Task.Delay(TimeSpan.FromMilliseconds(jitter));
            }
        }
    }

    public async Task BulkUpdateAsync(BulkJobUpdateRequest request)
    {
        await masterJobsService.BulkUpdateAsync(request);
    }

    public async Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs)
    {
        var result = await masterJobsService.BulkUpdateAsync(jobs);
        return result;
    }

    public async Task AddAsync(JobRawModel job)
    {
        await masterJobsService.AddAsync(job);
    }

    private bool ExistsJob(Guid jobId)
    {
        var job = masterJobsService.Get(jobId);
        return job != null;
    }
    
    private bool ExistsRecurringSchedule(Guid id)
    {
        var recurringSchedule = masterRecurringSchedulesService.Get(id);
        return recurringSchedule != null;
    }

    private async Task<bool> ExistsJobAsync(Guid jobId)
    {
        return await masterJobsService.GetAsync(jobId) != null;
    }

    private static int CalcJitterOnExecWithRetry(int attempt)
    {
        var minJitter = 5 * attempt;
        var maxJitter = minJitter + 100;
        var jitter = JobMasterRandomUtil.GetInt(minJitter, maxJitter);
        return jitter;
    }
}
