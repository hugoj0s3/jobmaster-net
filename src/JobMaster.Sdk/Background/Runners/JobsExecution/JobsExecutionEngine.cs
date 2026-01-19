using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using JobMaster.Contracts;
using JobMaster.Contracts.Models;
using JobMaster.Contracts.Models.Attributes;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Exceptions;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Jobs;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Services.Agent;
using JobMaster.Sdk.Contracts.Services.Master;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

public sealed class JobsExecutionEngine : IJobsExecutionEngine
{
    private readonly IJobMasterLogger logger;
    private readonly IMasterDistributedLockerService distributedLockerService;
    private readonly IJobMasterBackgroundAgentWorker backgroundAgentWorker;
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private readonly IMasterClusterConfigurationService masterClusterConfigurationService;
    private readonly IMasterJobsService masterJobsService;

    private readonly JobMasterLockKeys lockKeys;
    private readonly string bucketId;
    private readonly JobMasterPriority priority;
    
    public IOnBoardingControl<JobRawModel> OnBoardingControl { get; }
    public ITaskQueueControl<JobRawModel> TaskQueueControl { get; }

    public JobsExecutionEngine(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        string bucketId,
        JobMasterPriority priority)
    {
        this.backgroundAgentWorker = backgroundAgentWorker;
        this.distributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        this.masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        this.masterRecurringSchedulesService = backgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        this.masterClusterConfigurationService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
        this.masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        this.logger = backgroundAgentWorker.GetClusterAwareService<IJobMasterLogger>();
        
        this.lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
        this.bucketId = bucketId;
        this.priority = priority;
        
        this.OnBoardingControl = new OnBoardingControl<JobRawModel>(backgroundAgentWorker.BatchSize);
        this.TaskQueueControl = TaskQueueControl<JobRawModel>.Create(
            priority, 
            factor: backgroundAgentWorker.ParallelismFactor,
            preEnqueueAction: this.PreEnqueuedAsync);
    }
    
    public string BucketId => this.bucketId;
    public JobMasterPriority Priority => this.priority;
    
    public async Task<OnBoardingResult> TryOnBoardingJobAsync(JobRawModel payload, bool forceIfNoCapacity = false)
    {
        var now = DateTime.UtcNow;
        
        if (payload.ExceedProcessDeadline())
        {
            logger.Warn($"ExceedProcessDeadline. JobId={payload.Id} ScheduledAt={payload.ScheduledAt:O} now={now:O}", JobMasterLogSubjectType.Job, payload.Id);
            
            if (payload.CanHeldOnMasterExceedDeadline())
            {
                payload.MarkAsHeldOnMaster();
                await this.backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(payload));
                logger.Warn(
                    $"ExceedProcessDeadline. HeldOnMaster and terminated. JobId={payload.Id} ScheduledAt={payload.ScheduledAt:O} now={now:O}", JobMasterLogSubjectType.Job, payload.Id);
            }
           
            return OnBoardingResult.MovedToMaster;
        }
        
        // Check if job belongs to a cancelled recurring schedule
        if (payload.RecurringScheduleId.HasValue)
        {
            var (validationResult, _) = await ValidateRecurringScheduleAsync(
                payload.RecurringScheduleId.Value, 
                payload.ScheduledAt, 
                payload.Id);
            
            switch (validationResult)
            {
                case RecurringScheduleValidationResult.NotFound:
                    payload.MarkAsFailed();
                    await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(payload);
                    return OnBoardingResult.Cancelled;
                    
                case RecurringScheduleValidationResult.Terminated:
                    payload.TryToCancel(ignoreOnBoarding: true);
                    await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(payload);
                    return OnBoardingResult.Cancelled;
                    
                case RecurringScheduleValidationResult.StaticIdle:
                    payload.MarkAsHeldOnMaster();
                    await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(payload);
                    return OnBoardingResult.MovedToMaster;
            }
        }
        
        if (!payload.IsOnBoarding())
        {
            return OnBoardingResult.TooEarly;
        }
        
        if (payload.ProcessDeadline is null || !payload.Status.IsBucketStatus())
        {
            logger.Error($"Bad data", JobMasterLogSubjectType.Job, payload.Id); // TODO improve logo.
            return OnBoardingResult.Invalid;
        }
        
        // Try to buffer into OnBoarding. If briefly full, retry quickly before falling back.
        if (forceIfNoCapacity)
        {
            OnBoardingControl.ForcePush(payload, payload.Id.ToString(), payload.ScheduledAt, payload.ProcessDeadline!.Value);
            logger.Debug($"OnBoarding (forced): JobId={payload.Id}");
            return OnBoardingResult.Accepted;
        }

        var result = OnBoardingControl.Push(payload, payload.Id.ToString(), payload.ScheduledAt, payload.ProcessDeadline!.Value);
        if (result)
        {
            logger.Debug($"OnBoarding: JobId={payload.Id}");
            return OnBoardingResult.Accepted;
        }

        var onBoardingCount = OnBoardingControl.Count();
        var onBoardingCapacity = OnBoardingControl.CountAvailability() + onBoardingCount;
        this.logger.Warn($"OnBoarding Push failed. Count={onBoardingCount}, Capacity={onBoardingCapacity}, ProcessDeadline={payload.ProcessDeadline:O}, Now={DateTime.UtcNow:O}", JobMasterLogSubjectType.Bucket, BucketId);
        
        payload.MarkAsHeldOnMaster();
        await this.backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(payload));
        
        logger.Debug($"Moved to HeldOnMaster due to full OnBoarding: JobId={payload.Id}");

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        return OnBoardingResult.MovedToMaster;
    }

    public async Task PulseAsync()
    {
        var abortedCount = TaskQueueControl.AbortTimeoutTasks();
        var started = TaskQueueControl.StartQueuedTasksIfHasSlotAvailable();
        var shouldSkip = TaskQueueControl.CountAvailability() <= 0;
        
        if (abortedCount > 0)
        {
            logger.Debug($"Aborted {abortedCount} tasks due to timeout", JobMasterLogSubjectType.AgentWorker, backgroundAgentWorker.AgentWorkerId);
        }
        
        // Only log when tasks are actually started to reduce noise during idle periods
        if (started)
        {
            logger.Debug($"Started tasks this tick. running: {TaskQueueControl.CountRunning()}, waiting: {TaskQueueControl.CountWaiting()}, available: {TaskQueueControl.CountAvailability()}", JobMasterLogSubjectType.Bucket, BucketId);
        }
        
        // Only log when there are jobs in onboarding to reduce noise during idle periods
        if (OnBoardingControl.Count() > 0)
        {
            logger.Debug($"OnBoarding Count: {OnBoardingControl.Count()}", JobMasterLogSubjectType.Bucket, BucketId);
        }
        
        if (shouldSkip)
        {
            return;
        }

        var bucket = this.masterBucketsService.Get(bucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket?.Status != BucketStatus.Active)
        {
            var oldDepartureItems = OnBoardingControl.PruneOldDepartureItems(this.backgroundAgentWorker.BatchSize);
            foreach (var job in oldDepartureItems)
            {
                if (job.ExceedProcessDeadline())
                {
                    continue;
                }
                
                job.MarkAsHeldOnMaster();
                await this.backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(job));
            }
        }
        
        var expiredJobs = OnBoardingControl.PruneDeadlinedItems();
        foreach (var job in expiredJobs)
        {
            logger.Debug($"OnBoarding Pruning: Job {job.Id} removed from memory due to deadline.", JobMasterLogSubjectType.Job, job.Id);
            
            if (job.CanHeldOnMasterExceedDeadline())
            {
                job.MarkAsHeldOnMaster();
                await this.backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(job));
            }
        }
        
        if (bucket?.Status != BucketStatus.Active)
        {
            return;
        }
        
        var departureCapacity = TaskQueueControl.CountAvailability();
        var jobs = OnBoardingControl.GetReadyItems(DateTime.UtcNow, departureCapacity);
        logger.Debug($"DepartureItemsCount: {jobs.Count()} capacity={departureCapacity}", JobMasterLogSubjectType.Bucket, BucketId);
        if (OnBoardingControl.Count() > 0 && !jobs.Any())
        {
            var next = OnBoardingControl.PeekNextDepartureTime();
            logger.Debug($"OnBoarding has {OnBoardingControl.Count()} items but none ready. nextDeparture={next:O} now={DateTime.UtcNow:O}", JobMasterLogSubjectType.Bucket, BucketId);
        }
        
        foreach (var job in jobs)
        {
            if (job.ExceedProcessDeadline())
            {
                logger.Warn($"ExceedProcessDeadline. JobId={job.Id} ScheduledAt={job.ScheduledAt:O}", JobMasterLogSubjectType.Job, job.Id);
                
                if (job.CanHeldOnMasterExceedDeadline())
                {
                    job.MarkAsHeldOnMaster();
                    await this.backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(job));
                    logger.Warn($"ExceedProcessDeadline. HeldOnMaster and terminated. JobId={job.Id} ScheduledAt={job.ScheduledAt:O}", JobMasterLogSubjectType.Job, job.Id);
                }
                
                continue;
            }
            
            if (TaskQueueControl.Contains(job.Id.ToString()))
            {
                logger.Debug($"JobId={job.Id} already in TaskQueue");
                continue;
            }
            
            var taskQueueItem = new TaskQueueItem<JobRawModel>(
                job.Id.ToString(),
                job,
                job.Timeout,
                async token =>
                {
                    try
                    {
                        await this.ExecuteJobAsync(job, token);
                    }
                    finally
                    {
                        TaskQueueControl.StartQueuedTasksIfHasSlotAvailable();
                    }
                }
            );
                
            var added = await TaskQueueControl.EnqueueAsync(taskQueueItem);
            if (!added)
            {
                this.logger.Warn($"TaskQueue at limit. Re-balancing needed.", JobMasterLogSubjectType.Bucket, BucketId);

                if (!await this.masterJobsService.CheckVersionAsync(job.Id, job.Version))
                {
                    logger.Warn($"Job Conflict found, Probably processed by another node or HeldOnMaster. Excluded from queue.  JobId={job.Id} ScheduledAt={job.ScheduledAt:O}", JobMasterLogSubjectType.Job, job.Id);
                    continue;
                }
                
                if (job.ProcessDeadline is null)
                {
                    logger.Error($"Bad data", JobMasterLogSubjectType.Job, job.Id); // TODO improve logo.
                    continue;
                }
                
                OnBoardingControl.ForcePush(job, job.Id.ToString(), job.ScheduledAt, job.ProcessDeadline!.Value);
            }
        }
        
        // After enqueuing, try to start any queued tasks immediately
        TaskQueueControl.StartQueuedTasksIfHasSlotAvailable();
    }

    public async Task ExecuteJobAsync(JobRawModel jobRawModel, CancellationToken timeoutCancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        
        string? lockRecurringScheduleProcessingToken = null;
        var lockRecurringScheduleProcessingKey = jobRawModel.RecurringScheduleId.HasValue
            ? lockKeys.RecurringScheduleProcessingLock(jobRawModel.RecurringScheduleId.Value)
            : null;
        try
        {
            if (string.IsNullOrEmpty(this.bucketId))
            {
                return;
            }

            var config = this.masterClusterConfigurationService.Get();
            if (config == null || config.ClusterMode != ClusterMode.Active)
            {
                jobRawModel.MarkAsHeldOnMaster();
                await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(jobRawModel);
                return;
            }

            var bucket = masterBucketsService.Get(this.bucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
            if (bucket is null || (bucket.Status != BucketStatus.Active && bucket.Status != BucketStatus.Completing))
            {
                jobRawModel.MarkAsHeldOnMaster();
                await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(jobRawModel);
                return;
            }

            if (jobRawModel.ExceedProcessDeadline())
            {
                logger.Warn($"ExceedProcessDeadline. JobId={jobRawModel.Id} ScheduledAt={jobRawModel.ScheduledAt:O}", JobMasterLogSubjectType.Job, jobRawModel.Id);
                
                if (jobRawModel.CanHeldOnMasterExceedDeadline())
                {
                    jobRawModel.MarkAsHeldOnMaster();
                    await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(jobRawModel);
                    logger.Warn($"ExceedProcessDeadline. HeldOnMaster and terminated. JobId={jobRawModel.Id} ScheduledAt={jobRawModel.ScheduledAt:O}", JobMasterLogSubjectType.Job, jobRawModel.Id);
                }
                
                return;
            }
            
            try
            {
                RecurringScheduleContext? recurringScheduleContext = null;
                if (jobRawModel.RecurringScheduleId.HasValue)
                {
                    lockRecurringScheduleProcessingToken = distributedLockerService.TryLock(
                        lockRecurringScheduleProcessingKey!,
                        jobRawModel.Timeout.Add(TimeSpan.FromMinutes(1)));

                    if (lockRecurringScheduleProcessingToken == null)
                    {
                        logger.Warn($"Job overlap detected for recurring schedule {jobRawModel.RecurringScheduleId}", JobMasterLogSubjectType.RecurringSchedule, jobRawModel.RecurringScheduleId.Value);
                        logger.Warn($"Job overlap detected for recurring schedule {jobRawModel.RecurringScheduleId}", JobMasterLogSubjectType.JobExecution, jobRawModel.Id);
                        jobRawModel.MarkAsFailed();
                        await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(jobRawModel);
                        return;
                    }

                    // Check recurring schedule again at execution time (job may have been onboarded before cancellation)
                    var (validationResult, recurringSchedule) = await ValidateRecurringScheduleAsync(
                        jobRawModel.RecurringScheduleId.Value, 
                        jobRawModel.ScheduledAt, 
                        jobRawModel.Id);
                    
                    switch (validationResult)
                    {
                        case RecurringScheduleValidationResult.NotFound:
                            jobRawModel.MarkAsFailed();
                            await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(jobRawModel);
                            return;
                            
                        case RecurringScheduleValidationResult.Terminated:
                            jobRawModel.TryToCancel(ignoreOnBoarding: true);
                            await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(jobRawModel);
                            return;
                            
                        case RecurringScheduleValidationResult.StaticIdle:
                            backgroundAgentWorker.WorkerClusterOperations.MarkAsHeldOnMaster(jobRawModel.Id);
                            return;
                    }

                    recurringScheduleContext = RecurringScheduleConvertUtil.ToContext(recurringSchedule!);
                }
                
                timeoutCancellationToken.ThrowIfCancellationRequested();
                
                jobRawModel.ProcessingStarted();
                await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(jobRawModel);

                await using var scope = backgroundAgentWorker.ServiceProvider.CreateAsyncScope();
                var job = jobRawModel.ToJob();
                var jobContext = JobConvertUtil.ToJobContext(job);
                jobContext.RecurringSchedule = recurringScheduleContext;

                var handlerType = JobMasterDefinitionIdAttribute.GetJobHandlerTypeFromId(job.JobDefinitionId);
                if (handlerType is null)
                {
                    throw new InvalidOperationException($"Job handler type not found for job handler type id: {job.JobDefinitionId}");
                }

                var handler = scope.ServiceProvider.GetService(handlerType);
                if (handler is null)
                {
                    throw new InvalidOperationException(
                        $"Job handler type {handlerType.FullName} is not registered in the DI container. " +
                        $"Ensure the handler is in an assembly that is scanned during cluster configuration.");
                }

                if (handler is not IJobHandler jobHandler)
                {
                    throw new InvalidOperationException($"Job handler type {handlerType} does not implement IJobHandler");
                }

                timeoutCancellationToken.ThrowIfCancellationRequested();

                await jobHandler.HandleAsync(jobContext);

                jobRawModel.MarkAsSucceeded();
                await backgroundAgentWorker.WorkerClusterOperations
                    .ExecWithRetryAsync(o => o.Upsert(jobRawModel), millisecondsToDelay: 50);

                stopwatch.Stop();
                logger.Debug($"ExecuteJobAsync completed successfully in {stopwatch.ElapsedMilliseconds}ms", JobMasterLogSubjectType.Job, jobRawModel.Id);
            }
            catch (OperationCanceledException) when (timeoutCancellationToken.IsCancellationRequested)
            {
                stopwatch.Stop();
                string message = $"Job {jobRawModel.JobDefinitionId} timeout after {stopwatch.ElapsedMilliseconds}ms";
                if (!jobRawModel.TryRetry())
                {
                    message = $"Job {jobRawModel.JobDefinitionId} timeout after {stopwatch.ElapsedMilliseconds}ms. reached end of retries";
                }

                logger.Error(message, JobMasterLogSubjectType.JobExecution, jobRawModel.Id);

                await backgroundAgentWorker.WorkerClusterOperations
                    .ExecWithRetryAsync(o => o.Upsert(jobRawModel), millisecondsToDelay: 50);
            }
            catch (JobMasterVersionConflictException ce)
            {
                var existingJob = await this.masterJobsService.GetAsync(jobRawModel.Id);
                if (existingJob!.Status.IsFinalStatus())
                {
                    logger.Warn($"Job execution conflict Job is already in a final status ({existingJob.Status}). Executed by another process", JobMasterLogSubjectType.JobExecution, jobRawModel.Id);
                    return;
                }

                if (existingJob.Status == JobMasterJobStatus.HeldOnMaster)
                {
                    logger.Warn("Job execution conflict. Job is held on master", JobMasterLogSubjectType.JobExecution, jobRawModel.Id);
                    return;
                }

                logger.Error($"Job execution conflict. Job is probably running on another process. Trying to hold on master for safety. Status: ({existingJob.Status})", JobMasterLogSubjectType.JobExecution, jobRawModel.Id, exception: ce);
            }
            catch (Exception e)
            {
                stopwatch.Stop();
                await HandleErrorAsync(jobRawModel, stopwatch, e);
            }
            finally
            {
                var delay = GetPriorityDelay(priority);
                await RunnerDelayUtil.DelayAsync(delay, backgroundAgentWorker.CancellationTokenSource.Token);
            }
        }
        finally
        {
            if (lockRecurringScheduleProcessingKey != null)
            {
                distributedLockerService.ReleaseLock(lockRecurringScheduleProcessingKey, lockRecurringScheduleProcessingToken);
            }
        }
    }
    
    public async Task<bool> PreEnqueuedAsync(JobRawModel jobRawModel)
    {
        if (!jobRawModel.Status.IsBucketStatus())
        {
            logger.Error($"Job is not in a bucket status. Status: {jobRawModel.Status}", JobMasterLogSubjectType.Job, jobRawModel.Id);
            return false;
        }
        
        var originalStatus = jobRawModel.Status;
        jobRawModel.Enqueued();
        try
        {
            await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(jobRawModel);
            return true;
        }
        catch (JobMasterVersionConflictException)
        {
            jobRawModel.Status = originalStatus;
            
            var existingJob = await masterJobsService.GetAsync(jobRawModel.Id);
            if (existingJob?.Status == JobMasterJobStatus.Cancelled)
            {
                logger.Info($"Job {jobRawModel.Id} was cancelled before enqueued", JobMasterLogSubjectType.Job, jobRawModel.Id);
                return false;
            }
            
            logger.Warn($"" +
                        $"Job Conflict found Probably held on master or assigned to another bucket. {Environment.NewLine}" +
                        $"On Db Info(status: {existingJob?.Status}, bucketId: {existingJob?.BucketId}) {Environment.NewLine} " +
                        $"On this bucket Info: (status: {jobRawModel.Status}, bucketId: {jobRawModel.BucketId}).  JobId={jobRawModel.Id} ScheduledAt={jobRawModel.ScheduledAt:O}", 
                JobMasterLogSubjectType.Job, jobRawModel.Id);
            return false;
        }
    }
    
    private static TimeSpan GetPriorityDelay(JobMasterPriority priority, double factor = 1.0)
    {
        var baseDelay = priority switch
        {
            JobMasterPriority.VeryLow => TimeSpan.FromSeconds(1),
            JobMasterPriority.Low => TimeSpan.FromMilliseconds(750),
            JobMasterPriority.Medium => TimeSpan.FromMilliseconds(500),
            JobMasterPriority.High => TimeSpan.FromMilliseconds(250),
            JobMasterPriority.Critical => TimeSpan.FromMilliseconds(100),
            _ => TimeSpan.FromSeconds(1)
        };
        
        return TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * factor);
    }
    
    public async Task FlushToMasterAsync()
    {
        // 1. Collect everything from OnBoarding and waiting on queue
        // We use a high limit to ensure we grab everything in the pen
        var onBoardingJobs = OnBoardingControl.Shutdown();
        var waitingJobs = await TaskQueueControl.ShutdownAsync();
        
        var bufferedJobs = onBoardingJobs.Concat(waitingJobs).ToList();

        // 2. Collect everything from the TaskQueue waiting list (if applicable)
        // Note: Items already 'Running' in the TaskQueue are managed by their own 
        // cancellation tokens and should ideally finish or handle their own state.
        foreach (var job in bufferedJobs)
        {
            try 
            {
                if (job.ExceedProcessDeadline())
                {
                    // Skip expired buffered job and continue flushing the rest
                    continue;
                }
                
                // Reset state so they aren't 'Locked' to this dead worker
                job.MarkAsHeldOnMaster(); 
            
                await this.backgroundAgentWorker.WorkerClusterOperations
                    .ExecWithRetryAsync(o => o.Upsert(job));
            }
            catch (Exception ex)
            {
                this.logger.Error($"Failed to flush job {job.Id} during shutdown.", JobMasterLogSubjectType.Job, job.Id, ex);
            }
        }

        this.logger.Info($"Graceful flush complete for {BucketId}.");
    }

    private async Task HandleErrorAsync(JobRawModel job, Stopwatch stopwatch, Exception e)
    {
        string message = $"Job {job.JobDefinitionId} failed after {stopwatch.ElapsedMilliseconds}ms";
        if (!job.TryRetry())
        {
            message = $"Job {job.JobDefinitionId} failed after {stopwatch.ElapsedMilliseconds}ms. reached end of retries";
        } 
                
        logger.Error(message, JobMasterLogSubjectType.JobExecution, job.Id, exception: e);

        await backgroundAgentWorker.WorkerClusterOperations
            .ExecWithRetryAsync(o => o.Upsert(job), millisecondsToDelay: 50);
    }
    
    private enum RecurringScheduleValidationResult
    {
        Valid,
        NotFound,
        Terminated,
        StaticIdle
    }
    
    private async Task<(RecurringScheduleValidationResult result, RecurringScheduleRawModel? schedule)> ValidateRecurringScheduleAsync(
        Guid recurringScheduleId, 
        DateTime jobScheduledAt, 
        Guid jobId)
    {
        // For old jobs (>5 min in past), use UtcNow to prevent them from being valid indefinitely
        // For recent/future jobs, use their actual ScheduledAt time
        var dateToCheck = jobScheduledAt.AddMinutes(5) > DateTime.UtcNow ? jobScheduledAt : DateTime.UtcNow;
        var recurringSchedule = await masterRecurringSchedulesService.GetAsync(recurringScheduleId);
        
        if (recurringSchedule is null)
        {
            logger.Error($"Recurring schedule {recurringScheduleId} not found", JobMasterLogSubjectType.RecurringSchedule, recurringScheduleId);
            logger.Warn($"Recurring schedule {recurringScheduleId} not found", JobMasterLogSubjectType.JobExecution, jobId);
            return (RecurringScheduleValidationResult.NotFound, null);
        }

        if (recurringSchedule.Status.IsFinalStatus())
        {
            if (!recurringSchedule.TerminatedAt.HasValue)
            {
                recurringSchedule.TerminatedAt = DateTime.UtcNow;
                logger.Error("BAD DATA", JobMasterLogSubjectType.RecurringSchedule, recurringScheduleId); // TODO put a better message.
            }
            
            if (recurringSchedule.TerminatedAt.HasValue && recurringSchedule.TerminatedAt.Value > dateToCheck)
            {
                return (RecurringScheduleValidationResult.Valid, recurringSchedule);
            }
            
            logger.Warn($"Recurring schedule {recurringScheduleId} was terminated (canceled, inactive or completed)", JobMasterLogSubjectType.RecurringSchedule, recurringScheduleId);
            logger.Warn($"Recurring schedule {recurringScheduleId} was terminated (canceled, inactive or completed)", JobMasterLogSubjectType.JobExecution, jobId);
            return (RecurringScheduleValidationResult.Terminated, recurringSchedule);
        }

        if (recurringSchedule.IsStaticIdle(JobMasterRuntimeSingleton.Instance?.StartingAt))
        {
            logger.Warn($"Recurring schedule {recurringScheduleId} is static idle", JobMasterLogSubjectType.RecurringSchedule, recurringScheduleId);
            logger.Warn($"Recurring schedule {recurringScheduleId} is static idle", JobMasterLogSubjectType.JobExecution, jobId);
            return (RecurringScheduleValidationResult.StaticIdle, recurringSchedule);
        }

        return (RecurringScheduleValidationResult.Valid, recurringSchedule);
    }
}
