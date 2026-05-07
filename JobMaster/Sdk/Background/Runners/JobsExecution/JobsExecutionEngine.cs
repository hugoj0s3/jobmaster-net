using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Utils.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

internal sealed class JobsExecutionEngine : IJobsExecutionEngine
{
    private readonly IJobMasterLogger logger;
    private readonly IMasterDistributedLockerService distributedLockerService;
    private readonly IJobMasterBackgroundAgentWorker backgroundAgentWorker;
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private readonly IMasterClusterConfigurationService masterClusterConfigurationService;
    private readonly IMasterJobsService masterJobsService;

    private readonly List<JobRawModel> jobsToFlush = new();
    private readonly object jobsToFlushLock = new();
    private DateTime lastFlushedAtUtc = DateTime.MinValue;

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

        this.OnBoardingControl = new OnBoardingControl<JobRawModel>(backgroundAgentWorker.BucketBufferSize);
        this.TaskQueueControl = TaskQueueControl<JobRawModel>.Create(
            priority,
            factor: backgroundAgentWorker.ParallelismFactor,
            preEnqueueAction: this.PreEnqueuedAsync);
    }

    public string BucketId => this.bucketId;
    public JobMasterPriority Priority => this.priority;

    public int CountOnBoardingAvailability()
    {
        lock (jobsToFlushLock)
        {
            return OnBoardingControl.CountAvailability() - jobsToFlush.Count;
        }
    }

    public bool HasOnBoardingAvailability() => CountOnBoardingAvailability() > 0;

    public async Task<OnBoardingResult> TryOnBoardingJobAsync(JobRawModel payload, bool forceIfNoCapacity = false)
    {
        // Check if job belongs to a cancelled recurring schedule
        if (payload.SourceId.HasValue && payload.TriggerSourceType.IsRecurringTrigger())
        {
            var (validationResult, _) = await ValidateRecurringScheduleAsync(
                payload.SourceId.Value,
                payload.GetSafeNextPlanExecutionAt(),
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

        // Authorize the job — add to the staging list for deadline refresh + ForcePush on next flush.
        lock (jobsToFlushLock)
        {
            var available = OnBoardingControl.CountAvailability() - jobsToFlush.Count;
            if (forceIfNoCapacity || available > 0)
            {
                jobsToFlush.Add(payload);
                logger.Debug($"OnBoarding authorized: JobId={payload.Id} force={forceIfNoCapacity}");
                return OnBoardingResult.Accepted;
            }
        }

        logger.Debug($"Moved to HeldOnMaster due to full OnBoarding: JobId={payload.Id}");

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        return OnBoardingResult.Busy;
    }

    public async Task PulseAsync()
    {
        bool shouldFlush;
        lock (jobsToFlushLock)
        {
            shouldFlush = DateTime.UtcNow - lastFlushedAtUtc >= TimeSpan.FromSeconds(10)
                              || jobsToFlush.Count >= backgroundAgentWorker.BucketBufferSize;
        }

        if (shouldFlush)
        {
            await FlushToOnBoardingControlAsync();
            lastFlushedAtUtc = DateTime.UtcNow;
        }

        TaskQueueControl.StartQueuedTasksIfHasSlotAvailable();

        var bucket = this.masterBucketsService.Get(bucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket?.Status == BucketStatus.Completing)
        {
            await PullPendingJobsAsync();
        }

        if (bucket?.Status != BucketStatus.Active)
        {
            return;
        }

        if (TaskQueueControl.CountAvailability() <= 0)
        {
            return;
        }

        await EnqueueJobsAsync();

        // After enqueuing, try to start any queued tasks immediately
        TaskQueueControl.StartQueuedTasksIfHasSlotAvailable();
    }

    public async Task<bool> PreEnqueuedAsync(JobRawModel jobRawModel)
    {
        if (!jobRawModel.Status.IsBucketStatus())
        {
            logger.Error($"Job is not in a bucket status. Status: {jobRawModel.Status}", JobMasterLogSubjectType.Job, jobRawModel.Id);
            return false;
        }

        var originalStatus = jobRawModel.Status;
        jobRawModel.Enqueue();
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
                        $"On this bucket Info: (status: {jobRawModel.Status}, bucketId: {jobRawModel.BucketId}).  JobId={jobRawModel.Id} NextPlanExecutionAt={jobRawModel.NextPlanExecutionAt:O}",
                JobMasterLogSubjectType.Job, jobRawModel.Id);
            return false;
        }
    }

    public async Task FlushToMasterAsync()
    {
        // Collect onboarded jobs and waiting (not yet running) queued jobs.
        // Running jobs are managed by their own cancellation tokens and handle their own state.
        // Onboarded jobs already have ProcessDeadline = now, so HeldOnMasterDeadlineTimeoutJobsRunner
        // will recover them automatically — but we flush them here too for a faster handoff.
        var onBoardingJobs = OnBoardingControl.Shutdown();
        var waitingJobs = await TaskQueueControl.ShutdownAsync();

        List<JobRawModel> bufferedJobs;
        lock (jobsToFlushLock)
        {
            bufferedJobs = onBoardingJobs.Concat(waitingJobs).Concat(jobsToFlush).ToList();
        }
        
        if (bufferedJobs.Count == 0)
        {
            this.logger.Info($"Graceful flush complete for {BucketId}. No buffered jobs.");
            return;
        }

        foreach (var job in bufferedJobs)
        {
            job.MarkAsHeldOnMaster();
        }

        var partitions = bufferedJobs.Select(j => j.Id).ToList().Partition(JobMasterConstants.MaxBatchSizeForBulkOperation);
        foreach (var partition in partitions)
        {
            try
            {
                await masterJobsService.BulkUpdateAsync(BulkJobUpdateRequest.HeldOnMaster(partition.ToList()));
            }
            catch (Exception ex)
            {
                this.logger.Error($"Failed to flush jobs during shutdown for {BucketId}.", JobMasterLogSubjectType.Bucket, BucketId, ex);
            }
        }

        this.logger.Info($"Graceful flush complete for {BucketId}. Flushed {bufferedJobs.Count} jobs.");
    }

    private async Task FlushToOnBoardingControlAsync()
    {
        List<JobRawModel> batch;
        lock (jobsToFlushLock)
        {
            if (jobsToFlush.Count == 0)
            {
                return;
            }
            
            var take = Math.Min(jobsToFlush.Count, backgroundAgentWorker.BucketBufferSize);
            batch = jobsToFlush.GetRange(0, take);
            jobsToFlush.RemoveRange(0, take);
        }

        foreach (var job in batch)
        {
            job.Onboard();
        }

        foreach (var partition in batch.Partition(JobMasterConstants.MaxBatchSizeForBulkOperation))
        {
            var updated = await masterJobsService.BulkUpdateAsync(partition.ToList());

            foreach (var job in updated)
            {
                OnBoardingControl.Push(job, job.Id.ToString(), job.GetSafeNextPlanExecutionAt());
                logger.Debug($"OnBoarding flushed: JobId={job.Id}");
            }
        }
    }

    private async Task EnqueueJobsAsync()
    {
        var departureCapacity = TaskQueueControl.CountAvailability();
        var jobs = OnBoardingControl.GetReadyItems(DateTime.UtcNow, departureCapacity);

        foreach (var job in jobs)
        {
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
                    logger.Warn($"Job Conflict found, Probably processed by another node or HeldOnMaster. Excluded from queue.  JobId={job.Id} NextPlanExecutionAt={job.NextPlanExecutionAt:O}", JobMasterLogSubjectType.Job, job.Id);
                    continue;
                }

                OnBoardingControl.Push(job, job.Id.ToString(), job.GetSafeNextPlanExecutionAt());
            }
        }
    }

    private async Task PullPendingJobsAsync()
    {
        var pendingJobs = OnBoardingControl.PullPending(this.backgroundAgentWorker.BucketBufferSize);
        foreach (var job in pendingJobs)
        {
            if (job.ExceedProcessDeadline())
            {
                continue;
            }

            try
            {
                job.MarkAsHeldOnMaster();
                await this.backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(job));
            }
            catch (JobMasterVersionConflictException)
            {
                // Bucket may have transitioned to Lost while cache still shows Completing.
                // A drain/recovery runner likely already claimed this job — re-check bucket status and stop draining.
                logger.Warn($"Version conflict moving JobId={job.Id} to master during Completing drain. " +
                            $"Job likely already claimed by drain/recovery runner (bucket may have transitioned to Lost).",
                    JobMasterLogSubjectType.Job, job.Id);

                await Task.Delay(TimeSpan.FromSeconds(1));

                var bucket = this.masterBucketsService.Get(bucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
                if (bucket?.Status != BucketStatus.Completing)
                {
                    break;
                }

                logger.Warn($"Bucket still in Completing state after conflict on JobId={job.Id}. Conflict may be job-specific, continuing drain.", JobMasterLogSubjectType.Bucket, BucketId);
            }
        }
    }

    private async Task ExecuteJobAsync(JobRawModel jobRawModel, CancellationToken timeoutCancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        string? lockRecurringScheduleProcessingToken = null;
        var lockRecurringScheduleProcessingKey = jobRawModel.SourceId.HasValue && jobRawModel.TriggerSourceType.IsRecurringTrigger()
            ? lockKeys.RecurringScheduleProcessingLock(jobRawModel.SourceId.Value)
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
            
            JobExecution? execution = null;
            try
            {
                RecurringScheduleContext? recurringScheduleContext = null;
                if (jobRawModel.SourceId.HasValue && jobRawModel.TriggerSourceType.IsRecurringTrigger())
                {
                    lockRecurringScheduleProcessingToken = distributedLockerService.TryLock(
                        lockRecurringScheduleProcessingKey!,
                        jobRawModel.Timeout.Add(TimeSpan.FromMinutes(1)));

                    if (lockRecurringScheduleProcessingToken == null)
                    {
                        logger.Warn($"Job overlap detected for recurring schedule {jobRawModel.SourceId}", JobMasterLogSubjectType.RecurringSchedule, jobRawModel.SourceId.Value);
                        logger.Warn($"Job overlap detected for recurring schedule {jobRawModel.SourceId}", JobMasterLogSubjectType.JobExecution, jobRawModel.Id);
                        jobRawModel.MarkAsFailed();
                        await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(jobRawModel);
                        return;
                    }

                    // Check recurring schedule again at execution time (job may have been onboarded before cancellation)
                    var (validationResult, recurringSchedule) = await ValidateRecurringScheduleAsync(
                        jobRawModel.SourceId.Value,
                        jobRawModel.GetSafeNextPlanExecutionAt(),
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

                execution = jobRawModel.ProcessingStarted();
                await backgroundAgentWorker.WorkerClusterOperations.UpsertAsync(jobRawModel, execution);

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
                execution.Succeed();
                await backgroundAgentWorker.WorkerClusterOperations
                    .ExecWithRetryAsync(o => o.Upsert(jobRawModel, execution), millisecondsToDelay: 50);

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

                execution?.Fail(message);

                logger.Error(message, JobMasterLogSubjectType.JobExecution, jobRawModel.Id);

                await backgroundAgentWorker.WorkerClusterOperations
                    .ExecWithRetryAsync(o => o.Upsert(jobRawModel, execution), millisecondsToDelay: 50);
            }
            catch (JobMasterVersionConflictException ce)
            {
                var existingJob = await this.masterJobsService.GetAsync(jobRawModel.Id);
                if (existingJob!.Status.IsFinalStatus())
                {
                    logger.Warn($"Job execution conflict Job is already in a final status ({existingJob.Status}). Executed by another process", JobMasterLogSubjectType.JobExecution, jobRawModel.Id);
                    return;
                }

                if (existingJob.Status == JobMasterJobStatus.OnMaster)
                {
                    logger.Warn("Job execution conflict. Job is held on master", JobMasterLogSubjectType.JobExecution, jobRawModel.Id);
                    return;
                }

                logger.Error($"Job execution conflict. Job is probably running on another process. Trying to hold on master for safety. Status: ({existingJob.Status})", JobMasterLogSubjectType.JobExecution, jobRawModel.Id, exception: ce);

                if (execution != null)
                {
                    execution.Fail($"Job execution conflict. Job is probably running on another process. Trying to hold on master for safety. Status: ({existingJob.Status})");
                    await backgroundAgentWorker.WorkerClusterOperations
                        .ExecWithRetryAsync(o => o.SaveJobExecutionAsync(execution), millisecondsToDelay: 50);
                }
            }
            catch (Exception e)
            {
                stopwatch.Stop();
                await HandleErrorAsync(jobRawModel, execution, stopwatch, e);
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

    private async Task HandleErrorAsync(JobRawModel job, JobExecution? execution, Stopwatch stopwatch, Exception e)
    {
        string message = $"Job {job.JobDefinitionId} failed after {stopwatch.ElapsedMilliseconds}ms";
        if (!job.TryRetry())
        {
            message = $"Job {job.JobDefinitionId} failed after {stopwatch.ElapsedMilliseconds}ms. reached end of retries";
        }

        execution?.Fail(message);

        logger.Error(message, JobMasterLogSubjectType.JobExecution, job.Id, exception: e);

        await backgroundAgentWorker.WorkerClusterOperations
            .ExecWithRetryAsync(o => o.Upsert(job, execution), millisecondsToDelay: 50);
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
