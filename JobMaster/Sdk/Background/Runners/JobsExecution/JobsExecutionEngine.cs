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
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Utils.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

/// <summary>
/// Owns the full execution lifecycle of jobs assigned to a single bucket, from onboarding
/// through scheduling, execution, deadline enforcement, and drain.
///
/// <para><b>Onboarding</b> — <see cref="TryOnBoardingJobAsync"/> admits incoming jobs into
/// a staging list (<c>jobsToFlush</c>). Jobs for a <see cref="BucketStatus.Completing"/> bucket
/// are immediately redirected to master. Jobs present in <c>recentlyHeldOnMasterIdsGuard</c>
/// are skipped to prevent re-onboarding a job that was just moved to master, avoiding
/// version conflicts. The guard entry expires after <see cref="ErrorHoldDuration"/>.</para>
///
/// <para><b>Flush and scheduling</b> — on each pulse, <c>FlushToOnBoardingControlAsync</c>
/// bulk-persists the staging batch as <c>Onboarded</c> and pushes the jobs into
/// <see cref="OnBoardingControl"/>, which holds them in departure-time order until they
/// are ready to run.</para>
///
/// <para><b>Execution</b> — <see cref="EnqueueJobsAsync"/> dequeues ready jobs from
/// <see cref="OnBoardingControl"/> and submits them to <see cref="TaskQueueControl"/> for
/// concurrent execution. Each job runs through the full state machine: cluster-active guard
/// → recurring-schedule overlap lock → handler invocation → success / timeout / conflict /
/// error paths. On completion the runner re-acquires the engine lock before calling
/// <see cref="ITaskQueueControl{T}.StartQueuedTasksIfHasSlotAvailable"/>.</para>
///
/// <para><b>Deadline enforcement</b> — <see cref="CheckDeadlineJobsAsync"/> fires every
/// <see cref="JobMasterConstants.JobProcessDeadlineDefaultDuration"/>. It queries
/// deadline-exceeded jobs for this bucket, pre-excluding all IDs currently tracked by the
/// engine (staging, onboarding, task queue) so only genuinely untracked jobs are evaluated.
/// An in-loop safety guard handles any job that arrived in the engine between the exclusion
/// snapshot and the query response. Jobs that are lost (not tracked, bucket has capacity)
/// or in an invalid state are moved to <c>HeldOnMaster</c>. Jobs in a full bucket receive
/// a postpone whose duration is computed by <see cref="ComputePostponeTimeSpan"/>: average
/// running job timeout + <see cref="MinPostponeDuration"/> in normal mode, or
/// <see cref="MinPostponeDuration"/> alone when a recent pulse error is still within the
/// <see cref="ErrorHoldDuration"/> expiry window.</para>
///
/// <para><b>Completing drain</b> — when the bucket enters <see cref="BucketStatus.Completing"/>,
/// <c>PullPendingJobsAsync</c> flushes pending jobs from <see cref="OnBoardingControl"/>
/// back to master on each pulse. Once <see cref="IsIdle"/> confirms all four engine
/// components are empty (staging, onboarding, waiting, running), the bucket is transitioned
/// to <see cref="BucketStatus.ReadyToDrain"/>.</para>
///
/// <para><b>Graceful shutdown</b> — <see cref="FlushToMasterAsync"/> drains
/// <see cref="OnBoardingControl"/>, the waiting queue of <see cref="TaskQueueControl"/>,
/// and <c>jobsToFlush</c> back to master before the worker stops.</para>
///
/// <para><b>Concurrency</b> — all engine state mutations are serialized through an external
/// engine lock held by the runner, ensuring pulse and onboarding never overlap. The internal
/// <c>jobsToFlushLock</c> additionally guards the staging list for the
/// <see cref="CountOnBoardingAvailability"/> check, which may be called from outside the
/// engine lock.</para>
/// </summary>
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
    
    private readonly SemaphoreSlim engineLock = new(1, 1);
    private static readonly OperationThrottler singleJobUpdateThrottler = new(5, 500);

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
            preEnqueueAction: this.PreEnqueue);
    }

    public string BucketId => this.bucketId;
    public JobMasterPriority Priority => this.priority;

    /// <inheritdoc/>
    public int CountOnBoardingAvailability()
    {
        lock (jobsToFlushLock)
        {
            return OnBoardingControl.CountAvailability() - jobsToFlush.Count;
        }
    }

    /// <inheritdoc/>
    public bool HasOnBoardingAvailability() => CountOnBoardingAvailability() > 0;

    /// <inheritdoc/>
    public async Task<OnBoardingResult> TryOnBoardingJobAsync(JobRawModel payload, bool forceIfNoCapacity = false)
    {
        if (recentlyHeldOnMasterIdsGuard.ContainsKey(payload.Id) && 
            recentlyHeldOnMasterIdsGuard[payload.Id].Version == payload.Version)
        {
            return OnBoardingResult.MovedToMaster;
        }

        var bucket = masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket?.Status == BucketStatus.Completing)
        {
            payload.MarkAsHeldOnMaster();
            await this.UpdateSingleJobAsync(payload);
            return OnBoardingResult.MovedToMaster;
        }

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
                    await this.UpdateSingleJobAsync(payload);
                    return OnBoardingResult.Cancelled;

                case RecurringScheduleValidationResult.Terminated:
                    payload.TryToCancel(ignoreOnBoarding: true);
                    await this.UpdateSingleJobAsync(payload);
                    return OnBoardingResult.Cancelled;

                case RecurringScheduleValidationResult.StaticIdle:
                    payload.MarkAsHeldOnMaster();
                    await this.UpdateSingleJobAsync(payload);
                    return OnBoardingResult.MovedToMaster;
            }
        }

        if (!payload.IsWithinOnboardingWindow())
        {
            return OnBoardingResult.TooEarly;
        }

        if (payload.ProcessDeadline is null || !payload.Status.IsPreExecutionBucketStatus())
        {
            logger.Error($"Onboarding rejected: job has no ProcessDeadline or is not in a pre-execution status. Status={payload.Status} ProcessDeadline={payload.ProcessDeadline:O}", JobMasterLogCategory.Job, payload.Id);
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

        logger.Debug($"OnBoarding deferred — staging buffer full: JobId={payload.Id}");

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        return OnBoardingResult.Busy;
    }

    /// <inheritdoc/>
    public async Task PulseAsync()
    {
        await engineLock.WaitAsync();
        try
        {
            await PulseInternalAsync();
        }
        catch
        {
            lastErrorAtUtc = DateTime.UtcNow;
            throw;
        }
        finally
        {
            engineLock.Release();
        }
    }

    private async Task PulseInternalAsync()
    {
        await FlushToOnBoardingControlAsync();
        TaskQueueControl.StartQueuedTasksIfHasSlotAvailable();

        var bucket = this.masterBucketsService.Get(bucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket?.Status == BucketStatus.Active || 
            bucket?.Status == BucketStatus.Completing)
        {
            await CheckDeadlineJobsAsync();
        }
        
        if (bucket?.Status == BucketStatus.Completing)
        {
            await PullPendingJobsAsync();

            if (IsIdle())
            {
                await backgroundAgentWorker.WorkerClusterOperations.MarkBucketAsReadyToDrainAsync(bucketId);
            }
        }

        if (TaskQueueControl.CountAvailability() <= 0)
        {
            return;
        }

        await EnqueueJobsAsync();

        // After enqueuing, try to start any queued tasks immediately
        TaskQueueControl.StartQueuedTasksIfHasSlotAvailable();
    }

    /// <summary>
    /// Pre-enqueue hook called by <see cref="TaskQueueControl{T}"/> before a job is moved
    /// from the waiting queue into a running slot. Transitions the job to
    /// <see cref="JobMasterJobStatus.Queued"/> and persists the state change.
    /// Returns <see langword="false"/> if the job was concurrently cancelled or claimed by
    /// another node, in which case the item is dropped from the queue.
    /// </summary>
    private bool PreEnqueue(JobRawModel jobRawModel)
    {
        if (!jobRawModel.Status.IsPreExecutionBucketStatus())
        {
            logger.Error($"PreEnqueue rejected: job is not in a pre-execution status. Status={jobRawModel.Status}", JobMasterLogCategory.Job, jobRawModel.Id);
            return false;
        }

        var originalStatus = jobRawModel.Status;
        jobRawModel.Enqueue();
        try
        {
            this.UpdateSingleJob(jobRawModel);
            return true;
        }
        catch (JobMasterVersionConflictException)
        {
            jobRawModel.Status = originalStatus;

            var existingJob = masterJobsService.Get(jobRawModel.Id);
            if (existingJob?.Status == JobMasterJobStatus.Cancelled)
            {
                logger.Info($"Job {jobRawModel.Id} was cancelled before being enqueued.", JobMasterLogCategory.Job, jobRawModel.Id);
                return false;
            }

            logger.Warn(
                $"PreEnqueue conflict: job is probably held on master or assigned to another bucket. " +
                $"DB=(status:{existingJob?.Status}, bucketId:{existingJob?.BucketId}) " +
                $"Local=(status:{jobRawModel.Status}, bucketId:{jobRawModel.BucketId}) " +
                $"JobId={jobRawModel.Id} NextPlanExecutionAt={jobRawModel.NextPlanExecutionAt:O}",
                JobMasterLogCategory.Job, jobRawModel.Id);
            return false;
        }
    }

    /// <inheritdoc/>
    public async Task FlushToMasterAsync()
    {
        await engineLock.WaitAsync();
        try
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
                    await backgroundAgentWorker.WorkerClusterOperations
                        .ExecWithRetryAsync(o => o.BulkUpdateAsync(BulkJobUpdateRequest.HeldOnMaster(partition.ToList())));
                }
                catch (Exception ex)
                {
                    this.logger.Error($"Failed to flush jobs during shutdown for {BucketId}.", JobMasterLogCategory.Bucket, BucketId, ex);
                }
            }

            this.logger.Info($"Graceful flush complete for {BucketId}. Flushed {bufferedJobs.Count} jobs.");
        } 
        finally
        {
            engineLock.Release();
        }
    }

    /// <summary>
    /// Moves the current staging batch from <c>jobsToFlush</c> into <see cref="OnBoardingControl"/>.
    /// Each job is transitioned to <see cref="JobMasterJobStatus.Onboarded"/> and persisted via
    /// a bulk update before being pushed. Only jobs returned by the bulk update (confirming
    /// successful persistence) are admitted to the onboarding queue.
    /// </summary>
    private async Task FlushToOnBoardingControlAsync()
    {
        List<JobRawModel> batch;
        lock (jobsToFlushLock)
        {
           var shouldFlush = DateTime.UtcNow - lastFlushedAtUtc >= TimeSpan.FromSeconds(10)
                          || jobsToFlush.Count >= backgroundAgentWorker.BucketBufferSize;
           
            if (!shouldFlush)
            {
                return;
            }

            if (jobsToFlush.Count == 0)
            {
                return;
            }

            lastFlushedAtUtc = DateTime.UtcNow;
            
            var take = Math.Min(jobsToFlush.Count, backgroundAgentWorker.BucketBufferSize);
            batch = jobsToFlush.GetRange(0, take);
            jobsToFlush.RemoveRange(0, take);
        }

        var bucket = this.masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket?.Status == BucketStatus.Active)
        {
            foreach (var job in batch)
            {
                job.Onboard();
            }

            foreach (var partition in batch.Partition(JobMasterConstants.MaxBatchSizeForBulkOperation))
            {
                var updated = await backgroundAgentWorker.WorkerClusterOperations
                    .ExecWithRetryAsync(o => o.BulkUpdateAsync(partition.ToList()));

                foreach (var job in updated)
                {
                    OnBoardingControl.Push(job, job.Id.ToString(), job.GetSafeNextPlanExecutionAt());
                    logger.Debug($"OnBoarding flushed: JobId={job.Id}");
                }
            }
        }
        else
        {
            foreach (var partition in batch.Partition(JobMasterConstants.MaxBatchSizeForBulkOperation))
            {
                var idsToHeld = partition.Select(x => x.Id).ToList();
                var heldOnMasterReq = BulkJobUpdateRequest.HeldOnMaster(idsToHeld);

                await backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o =>
                    o.BulkUpdateAsync(heldOnMasterReq));
            }
        }
    }

    /// <summary>
    /// Dequeues jobs that have reached their departure time from <see cref="OnBoardingControl"/>
    /// and submits them to <see cref="TaskQueueControl{T}"/> for concurrent execution.
    /// Jobs that cannot be enqueued (queue at capacity) are pushed back to
    /// <see cref="OnBoardingControl"/> after a version check to confirm they are still valid.
    /// </summary>
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
                        var acquired = await engineLock.WaitAsync(TimeSpan.FromSeconds(1), token);
                        if (acquired)
                        {
                            TaskQueueControl.StartQueuedTasksIfHasSlotAvailable();
                            engineLock.Release();
                        }
                    }
                },
                cancellationTimeout: job.Timeout.Add(TimeSpan.FromSeconds(2.5)));

            var added = TaskQueueControl.Enqueue(taskQueueItem);
            if (!added)
            {
                this.logger.Warn($"TaskQueue at limit for bucket {BucketId}. Re-balancing needed.", JobMasterLogCategory.Bucket, BucketId);

                if (!await this.masterJobsService.CheckVersionAsync(job.Id, job.Version))
                {
                    logger.Warn($"Job conflict detected — likely processed by another node or held on master. Excluded from queue. JobId={job.Id} NextPlanExecutionAt={job.NextPlanExecutionAt:O}", JobMasterLogCategory.Job, job.Id);
                    continue;
                }

                OnBoardingControl.Push(job, job.Id.ToString(), job.GetSafeNextPlanExecutionAt());
            }
        }
    }
    
    private const int PostponeFactor = 4;
    private static readonly TimeSpan MinPostponeDuration =
        TimeSpan.FromTicks(JobMasterConstants.JobProcessDeadlineDefaultDuration.Ticks / PostponeFactor);
    private static readonly TimeSpan ErrorHoldDuration =
        TimeSpan.FromTicks(JobMasterConstants.JobProcessDeadlineDefaultDuration.Ticks * PostponeFactor);

    private static readonly TimeSpan ReVerifyDuration =
        TimeSpan.FromTicks(JobMasterConstants.JobProcessDeadlineDefaultDuration.Ticks * 2);

    private readonly HashSet<Guid> firstPostponedIds = new();
    private DateTime lastDeadlineCheckAtUtc = DateTime.MinValue;
    private readonly Dictionary<Guid, RecentlyHeldOnMaster> recentlyHeldOnMasterIdsGuard = new();
    private DateTime lastErrorAtUtc = DateTime.MinValue;
    private async Task CheckDeadlineJobsAsync()
    {
        var nowUtc = DateTime.UtcNow;
        if (nowUtc - lastDeadlineCheckAtUtc < JobMasterConstants.JobProcessDeadlineDefaultDuration)
            return;
        lastDeadlineCheckAtUtc = nowUtc;
        
        recentlyHeldOnMasterIdsGuard
            .Where(kv => kv.Value.ExpiresAt <= nowUtc)
            .Select(kv => kv.Key).ToList()
            .ForEach(id => recentlyHeldOnMasterIdsGuard.Remove(id));
        
        List<Guid> idsToExclude;
        lock (jobsToFlushLock)
        {
            idsToExclude = jobsToFlush
                .Select(j => j.Id)
                .Concat(OnBoardingControl.GetIds().Select(Guid.Parse))
                .Concat(TaskQueueControl.GetIds().Select(Guid.Parse))
                .Distinct()
                .ToList();
        }

        var jobs = await masterJobsService.QueryAsync(new JobQueryCriteria
        {
            BucketId = bucketId,
            ProcessDeadlineTo = nowUtc,
            CountLimit = backgroundAgentWorker.BucketBufferSize,
            SortBy = new SortByCriteria()
            {
                Ascending = true,
                Property = nameof(JobRawModel.ProcessDeadline),
            },
            ExcludeJobIds = idsToExclude,
        });

        // Clean up IDs that are no longer deadline-exceeded (successfully processed between checks)
        var currentJobIds = jobs.Select(j => j.Id).ToList();
        firstPostponedIds.RemoveWhere(id => !currentJobIds.Contains(id));

        if (jobs.Count == 0)
            return;

        var toHoldOnMaster = new List<JobRawModel>();
        var postponedJobs = new List<JobRawModel>();

        var bucket = masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);

        foreach (var job in jobs)
        {
            if (!job.Status.IsBucketStatus())
            {
                logger.Error($"Deadline check: job is in an unexpected status (not a bucket status). Holding on master. Status={job.Status}", JobMasterLogCategory.Job, job.Id);
                toHoldOnMaster.Add(job);
            }
            else
            {
                if (TaskQueueControl.Contains(job.Id.ToString()) ||
                    OnBoardingControl.Contains(job.Id.ToString()) ||
                    IsFlushPending(job.Id))
                {
                    continue;
                }

                if (bucket?.Status != BucketStatus.Active)
                {
                    toHoldOnMaster.Add(job);
                    continue;
                }

                // If the Job is not InBucket it should be on TaskQueueControl or OnBoardingControl list.
                // So it is in invalid state, need to held on master. probably error happened before.
                if (job.Status != JobMasterJobStatus.InBucket)
                {
                    toHoldOnMaster.Add(job);
                    continue;
                }
                
                if (firstPostponedIds.Remove(job.Id))
                {
                    toHoldOnMaster.Add(job);
                    continue;
                }

                // Has capacity but job is not tracked → error condition (job lost from engine)
                // No capacity → bucket is busy → safe to postpone
                if (TaskQueueControl.CountAvailability() > 0 || CountOnBoardingAvailability() > 0)
                {
                    toHoldOnMaster.Add(job);
                    continue;
                }

                postponedJobs.Add(job);
            }
        }

        var postponeTimeSpan = ComputePostponeTimeSpan();
        foreach (var job in postponedJobs)
        {
            job.RefreshDeadline(postponeTimeSpan);
        }

        foreach (var partition
                 in postponedJobs.Partition(JobMasterConstants.MaxBatchSizeForBulkOperation))
        {
            await backgroundAgentWorker.WorkerClusterOperations
                .ExecWithRetryAsync(o => o.BulkUpdateAsync(partition.ToList()));

            foreach (var job in partition)
            {
                firstPostponedIds.Add(job.Id);
            }
        }

        foreach (var job in toHoldOnMaster)
        {
            job.MarkAsHeldOnMaster();
        }

        foreach (var partition in toHoldOnMaster.ToList()
                     .Partition(JobMasterConstants.MaxBatchSizeForBulkOperation))
        {
            var partitionIds =  partition.Select(j => j.Id).ToList();
            await backgroundAgentWorker.WorkerClusterOperations
                .ExecWithRetryAsync(o => o.BulkUpdateAsync(
                    BulkJobUpdateRequest.HeldOnMaster(partitionIds)));
            
            foreach (var job in partition)
            {
                recentlyHeldOnMasterIdsGuard[job.Id] = new RecentlyHeldOnMaster()
                {
                    JobId = job.Id,
                    Version = job.Version,
                    ExpiresAt = DateTime.UtcNow.Add(ErrorHoldDuration),
                };
            }
        }

        if (toHoldOnMaster.Count > 0)
        {
            logger.Warn(
                $"CheckDeadlineJobsAsync: Moved {toHoldOnMaster.Count} deadline-exceeded jobs to master. " +
                $"BucketId={bucketId} JobIds={string.Join(", ", toHoldOnMaster.Select(j => j.Id).Take(10))}",
                JobMasterLogCategory.Bucket, bucketId);
        } 
    }

    /// <summary>
    /// Drains pending (not-yet-executing) jobs from <see cref="OnBoardingControl"/> back to
    /// the master when the bucket is in <see cref="BucketStatus.Completing"/> state.
    /// Version conflicts during update indicate another runner already claimed the job;
    /// if the bucket is no longer Completing, draining stops early.
    /// </summary>
    private async Task PullPendingJobsAsync()
    {
        var pendingJobs = OnBoardingControl.PullPending(this.backgroundAgentWorker.BucketBufferSize);
        foreach (var job in pendingJobs)
        {
            try
            {
                job.MarkAsHeldOnMaster();
                await this.UpdateSingleJobAsync(job);
            }
            catch (JobMasterVersionConflictException)
            {
                // Bucket may have transitioned to Lost while cache still shows Completing.
                // A drain/recovery runner likely already claimed this job — re-check bucket status and stop draining.
                logger.Warn($"Version conflict moving JobId={job.Id} to master during Completing drain. " +
                            $"Job likely already claimed by drain/recovery runner (bucket may have transitioned to Lost).",
                    JobMasterLogCategory.Job, job.Id);

                await Task.Delay(TimeSpan.FromSeconds(1));

                var bucket = this.masterBucketsService.Get(bucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
                if (bucket?.Status != BucketStatus.Completing)
                {
                    break;
                }

                logger.Warn($"Bucket still in Completing state after conflict on JobId={job.Id}. Conflict may be job-specific, continuing drain.", JobMasterLogCategory.Bucket, BucketId);
            }
        }
    }

    /// <summary>
    /// Resolves and invokes the handler for a single job, managing the full execution
    /// state machine: cluster-active guard → recurring-schedule overlap lock → handler
    /// invocation → success/timeout/conflict/error paths. The priority-based inter-job
    /// delay is applied in the <c>finally</c> block regardless of outcome.
    /// </summary>
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
                await this.UpdateSingleJobAsync(jobRawModel);
                return;
            }

            var bucket = masterBucketsService.Get(this.bucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
            if (bucket is null || (bucket.Status != BucketStatus.Active && bucket.Status != BucketStatus.Completing))
            {
                jobRawModel.MarkAsHeldOnMaster();
                await this.UpdateSingleJobAsync(jobRawModel);
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
                        logger.Warn($"Recurring schedule {jobRawModel.SourceId} has an overlapping job still executing.", JobMasterLogCategory.RecurringSchedule, jobRawModel.SourceId.Value);
                        logger.Warn($"Job {jobRawModel.Id} failed due to overlap — recurring schedule {jobRawModel.SourceId} is still executing.", JobMasterLogCategory.JobExecution, jobRawModel.Id);
                        jobRawModel.MarkAsFailed();
                        await this.UpdateSingleJobAsync(jobRawModel);
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
                            await this.UpdateSingleJobAsync(jobRawModel);
                            return;

                        case RecurringScheduleValidationResult.Terminated:
                            jobRawModel.TryToCancel(ignoreOnBoarding: true);
                            await this.UpdateSingleJobAsync(jobRawModel);
                            return;

                        case RecurringScheduleValidationResult.StaticIdle:
                            backgroundAgentWorker.WorkerClusterOperations.MarkAsHeldOnMaster(jobRawModel.Id);
                            return;
                    }

                    recurringScheduleContext = RecurringScheduleConvertUtil.ToContext(recurringSchedule!);
                }

                timeoutCancellationToken.ThrowIfCancellationRequested();

                execution = jobRawModel.ProcessingStarted();
                await this.UpdateSingleJobAsync(jobRawModel);

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
                await this.UpdateSingleJobAsync(jobRawModel, execution);

                stopwatch.Stop();
                logger.Debug($"ExecuteJobAsync completed successfully in {stopwatch.ElapsedMilliseconds}ms", JobMasterLogCategory.Job, jobRawModel.Id);
            }
            catch (OperationCanceledException) when (timeoutCancellationToken.IsCancellationRequested)
            {
                stopwatch.Stop();
                string message = $"Job {jobRawModel.JobDefinitionId} timed out after {stopwatch.ElapsedMilliseconds}ms.";
                if (!jobRawModel.TryRetry())
                {
                    message = $"Job {jobRawModel.JobDefinitionId} timed out after {stopwatch.ElapsedMilliseconds}ms. Reached max retries.";
                }

                execution?.Fail(message);

                logger.Error(message, JobMasterLogCategory.JobExecution, jobRawModel.Id);

                await this.UpdateSingleJobAsync(jobRawModel, execution);
            }
            catch (JobMasterVersionConflictException ce)
            {
                var existingJob = await this.masterJobsService.GetAsync(jobRawModel.Id);
                if (existingJob!.Status.IsFinalStatus())
                {
                    logger.Warn($"Job execution conflict: job is already in a final status ({existingJob.Status}). Likely executed by another process.", JobMasterLogCategory.JobExecution, jobRawModel.Id);
                    return;
                }

                if (existingJob.Status == JobMasterJobStatus.OnMaster)
                {
                    logger.Warn("Job execution conflict. Job is held on master", JobMasterLogCategory.JobExecution, jobRawModel.Id);
                    return;
                }

                logger.Error($"Job execution conflict. Job is probably running on another process. Trying to hold on master for safety. Status: ({existingJob.Status})", JobMasterLogCategory.JobExecution, jobRawModel.Id, exception: ce);

                if (execution != null)
                {
                    execution.Fail($"Job execution conflict. Job is probably running on another process. Trying to hold on master for safety. Status: ({existingJob.Status})");
                    await backgroundAgentWorker.WorkerClusterOperations
                        .ExecWithRetryAsync(o => o.AddJobExecutionAsync(execution), millisecondsToDelay: 50);
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
    
    private bool IsIdle()
    {
        lock (jobsToFlushLock)
        {
            return jobsToFlush.Count == 0
                   && OnBoardingControl.CountItems() == 0
                   && TaskQueueControl.CountRunning() == 0
                   && TaskQueueControl.CountWaiting() == 0;
        }
    }

    private bool IsFlushPending(Guid jobId)
    {
        lock (jobsToFlushLock)
        {
            return jobsToFlush.Any(j => j.Id == jobId);
        }
    }

    /// <summary>
    /// Computes the postpone duration for deadline-exceeded jobs that are safe to defer.
    /// In error mode (a recent <see cref="PulseAsync"/> failure within <see cref="ErrorHoldDuration"/>)
    /// returns <see cref="MinPostponeDuration"/> to retry quickly without overwhelming a struggling engine.
    /// In normal mode the average running job timeout * PostponeFactor is added as a buffer so the job is not recalled
    /// before the current cohort is likely to have finished.
    /// Falls back to <see cref="MinPostponeDuration"/> when nothing is currently running.
    /// </summary>
    private TimeSpan ComputePostponeTimeSpan()
    {
        if (lastErrorAtUtc != DateTime.MinValue && DateTime.UtcNow < lastErrorAtUtc + ErrorHoldDuration)
        {
            return MinPostponeDuration;
        }

        var runningTimeouts = TaskQueueControl.GetRunningTimeouts().ToList();
        if (runningTimeouts.Count == 0)
        {
            return MinPostponeDuration;
        }

        var avgTicks = (long)runningTimeouts.Average(t => t.Ticks) * PostponeFactor;
        return TimeSpan.FromTicks(avgTicks) + JobMasterConstants.JobProcessDeadlineDefaultDuration;
    }

    /// <summary>
    /// Returns the inter-job execution delay for the given priority level, scaled by
    /// <paramref name="factor"/>. Higher-priority buckets use shorter delays to achieve
    /// greater throughput.
    /// </summary>
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

    /// <summary>
    /// Handles an unexpected exception from job execution: logs the failure, increments
    /// the retry counter (or marks the job as permanently failed if retries are exhausted),
    /// records the failed execution, and persists the updated state.
    /// </summary>
    private async Task HandleErrorAsync(JobRawModel job, JobExecution? execution, Stopwatch stopwatch, Exception e)
    {
        string message = $"Job {job.JobDefinitionId} failed after {stopwatch.ElapsedMilliseconds}ms.";
        if (!job.TryRetry())
        {
            message = $"Job {job.JobDefinitionId} failed after {stopwatch.ElapsedMilliseconds}ms. Reached max retries.";
        }

        execution?.Fail(message);

        logger.Error(message, JobMasterLogCategory.JobExecution, job.Id, exception: e);

        await this.UpdateSingleJobAsync(job, execution);
    }

    private enum RecurringScheduleValidationResult
    {
        Valid,
        NotFound,
        Terminated,
        StaticIdle
    }

    /// <summary>
    /// Validates that a recurring schedule is still eligible to run a job at
    /// <paramref name="jobScheduledAt"/>. Returns one of four outcomes:
    /// <see cref="RecurringScheduleValidationResult.Valid"/> if execution should proceed,
    /// <see cref="RecurringScheduleValidationResult.NotFound"/> if the schedule no longer exists,
    /// <see cref="RecurringScheduleValidationResult.Terminated"/> if it ended before the job's
    /// scheduled time, or <see cref="RecurringScheduleValidationResult.StaticIdle"/> if the
    /// static definition's keep-alive has lapsed.
    /// </summary>
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
            logger.Error($"Recurring schedule {recurringScheduleId} not found", JobMasterLogCategory.RecurringSchedule, recurringScheduleId);
            logger.Warn($"Recurring schedule {recurringScheduleId} not found", JobMasterLogCategory.JobExecution, jobId);
            return (RecurringScheduleValidationResult.NotFound, null);
        }

        if (recurringSchedule.Status.IsFinalStatus())
        {
            if (!recurringSchedule.TerminatedAt.HasValue)
            {
                recurringSchedule.TerminatedAt = DateTime.UtcNow;
                logger.Error($"Recurring schedule {recurringScheduleId} is in a final status but has no TerminatedAt set. Using UtcNow as fallback.", JobMasterLogCategory.RecurringSchedule, recurringScheduleId);
            }

            if (recurringSchedule.TerminatedAt.HasValue && recurringSchedule.TerminatedAt.Value > dateToCheck)
            {
                return (RecurringScheduleValidationResult.Valid, recurringSchedule);
            }

            logger.Warn($"Recurring schedule {recurringScheduleId} was terminated (cancelled, inactive, or completed).", JobMasterLogCategory.RecurringSchedule, recurringScheduleId);
            logger.Warn($"Job {jobId} cancelled because recurring schedule {recurringScheduleId} was terminated (cancelled, inactive, or completed).", JobMasterLogCategory.JobExecution, jobId);
            return (RecurringScheduleValidationResult.Terminated, recurringSchedule);
        }

        if (recurringSchedule.IsStaticIdle(JobMasterRuntimeSingleton.Instance?.StartingAt))
        {
            logger.Warn($"Recurring schedule {recurringScheduleId} is static idle.", JobMasterLogCategory.RecurringSchedule, recurringScheduleId);
            logger.Warn($"Job {jobId} held on master because recurring schedule {recurringScheduleId} is static idle.", JobMasterLogCategory.JobExecution, jobId);
            return (RecurringScheduleValidationResult.StaticIdle, recurringSchedule);
        }

        return (RecurringScheduleValidationResult.Valid, recurringSchedule);
    }

    private async Task UpdateSingleJobAsync(JobRawModel jobRawModel, JobExecution? execution = null)
    {
        await singleJobUpdateThrottler.ExecAsync(() =>
            this.backgroundAgentWorker.WorkerClusterOperations
                .ExecWithRetryAsync(async o => await o.UpdateAsync(jobRawModel, execution), millisecondsToDelay: 50));
    }

    private void UpdateSingleJob(JobRawModel jobRawModel)
    {
        singleJobUpdateThrottler.Exec(() =>
            this.backgroundAgentWorker.WorkerClusterOperations.Update(jobRawModel));
    }

    private class RecentlyHeldOnMaster
    {
        public Guid JobId { get; set; }
        public string? Version { get; set; }
        
        public DateTime ExpiresAt { get; set; }
    }
}
