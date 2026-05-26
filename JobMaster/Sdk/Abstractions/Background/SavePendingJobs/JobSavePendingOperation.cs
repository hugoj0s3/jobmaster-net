using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Abstractions.Background.SavePendingJobs;

/// <summary>
/// Encapsulates the logic for persisting a <see cref="JobRawModel"/> that arrives in the
/// <c>PendingSave</c> state, covering both normal ingestion and bucket-drain scenarios.
/// <para>
/// Depending on context, the operation either: holds the job on the master (HeldOnMaster),
/// short-circuits it directly into the local <c>JobsExecutionEngine</c>, or assigns it to a
/// selected bucket and publishes it to the agent dispatcher for remote execution.
/// </para>
/// </summary>
internal class JobSavePendingOperation
{
    
    protected readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    protected readonly IMasterJobsService masterJobsService;
    protected readonly IMasterBucketsService masterBucketsService;
    protected readonly IWorkerClusterOperations workerClusterOperations;
    protected readonly IJobMasterLogger logger;
    protected readonly IJobMasterBackgroundAgentWorker backgroundAgentWorker;
    private readonly string bucketId;

    /// <summary>
    /// Initializes the operation by resolving all required cluster-aware services from the
    /// supplied <paramref name="backgroundAgentWorker"/> and binding it to the given
    /// <paramref name="bucketId"/>.
    /// </summary>
    /// <param name="backgroundAgentWorker">
    /// The background agent worker that provides cluster-scoped service resolution and engine access.
    /// </param>
    /// <param name="bucketId">
    /// The bucket this operation is associated with, used for engine look-ups and fallback bucket retrieval.
    /// </param>
    public JobSavePendingOperation(IJobMasterBackgroundAgentWorker backgroundAgentWorker, string bucketId)
    {
        
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        workerClusterOperations = backgroundAgentWorker.GetClusterAwareService<IWorkerClusterOperations>();
        logger = backgroundAgentWorker.GetClusterAwareService<IJobMasterLogger>();

        this.backgroundAgentWorker = backgroundAgentWorker;
        this.bucketId = bucketId;
    }
    
    /// <summary>
    /// Persists a job as <c>HeldOnMaster</c> during a bucket drain, with a fallback safe-guard
    /// that re-enqueues the job via the agent dispatcher if the primary upsert fails.
    /// </summary>
    /// <remarks>
    /// On a duplication exception the job is considered already handled and
    /// <see cref="SaveDrainResultCode.AlreadyExists"/> is returned without re-enqueuing.
    /// On any other failure the safe-guard attempts to reassign the job to the current bucket
    /// and dispatch it; if that also fails the result is <see cref="SaveDrainResultCode.Failed"/>.
    /// </remarks>
    /// <param name="job">The job to persist.</param>
    /// <returns>
    /// <see cref="SaveDrainResultCode.Success"/> on a clean persist;
    /// <see cref="SaveDrainResultCode.AlreadyExists"/> on duplication;
    /// <see cref="SaveDrainResultCode.FailedSafeGuarded"/> when the primary write failed but the
    /// safe-guard dispatch succeeded; or <see cref="SaveDrainResultCode.Failed"/> when both paths fail.
    /// </returns>
    public async Task<SaveDrainResultCode> AddPendingSaveJobForDrainWithSafeGuardAsync(JobRawModel job)
    {
        try
        {
            job.MarkAsHeldOnMaster();
            await backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(job));
            return SaveDrainResultCode.Success;
        }
        catch (JobMasterDuplicationException e)
        {
            logger.Warn("Job duplication detected", JobMasterLogCategory.Job, job.Id, exception: e);
            return SaveDrainResultCode.AlreadyExists;
        }
        catch
        {
            logger.Error("Failed to hold job on master", JobMasterLogCategory.Job, job.Id);
            try
            {
                var bucket = masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
                if (bucket is null)
                {
                    logger.Critical($"No bucket available to reassign job during drain safeguard. Job: {job.ToLogSummary()}", JobMasterLogCategory.Job, job.Id);
                    return SaveDrainResultCode.Failed;
                }
                job.AssignSavePendingJobToBucket(bucket);
                await agentJobsDispatcherService.AddSavePendingJobAsync(job);
                return SaveDrainResultCode.FailedSafeGuarded;
            }
            catch (Exception e)
            {
                logger.Critical("Failed to add job to queue", JobMasterLogCategory.Job, job.Id, exception: e);
                return SaveDrainResultCode.Failed;
            }
        }
    }
    
    /// <summary>
    /// Persists a job as <c>HeldOnMaster</c> during a bucket drain using an insert (not upsert).
    /// Unlike <see cref="AddPendingSaveJobForDrainWithSafeGuardAsync"/>, this method has no
    /// fallback re-dispatch path on failure.
    /// </summary>
    /// <param name="job">The job to persist.</param>
    /// <returns>
    /// <see cref="SaveDrainResultCode.Success"/> on a clean insert;
    /// <see cref="SaveDrainResultCode.AlreadyExists"/> on duplication;
    /// or <see cref="SaveDrainResultCode.Failed"/> on any other error.
    /// </returns>
    public async Task<SaveDrainResultCode> AddPendingSaveJobForDrainAsync(JobRawModel job)
    {
        try
        { 
            job.MarkAsHeldOnMaster();
            await backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.AddAsync(job));
            return SaveDrainResultCode.Success;
        }
        catch (JobMasterDuplicationException dupE)
        {
            logger.Warn("Job duplication detected", JobMasterLogCategory.Job, job.Id, exception: dupE);
            return SaveDrainResultCode.AlreadyExists;
        }
        catch (Exception ex)
        {
            logger.Error("Failed to hold job on master", JobMasterLogCategory.Job, job.Id, exception: ex);
            return SaveDrainResultCode.Failed;
        }
    }
    
    /// <summary>
    /// Transitions a job to <c>HeldOnMaster</c> via an upsert during bucket drain processing.
    /// A version-conflict exception is treated as a benign race — another runner already claimed
    /// or updated the job — and returns <see cref="SaveDrainResultCode.Skipped"/>.
    /// </summary>
    /// <param name="job">The job to mark as held on master.</param>
    /// <returns>
    /// <see cref="SaveDrainResultCode.Success"/> on a clean upsert;
    /// <see cref="SaveDrainResultCode.Skipped"/> on a version conflict;
    /// or <see cref="SaveDrainResultCode.Failed"/> on any other error.
    /// </returns>
    public async Task<SaveDrainResultCode> HeldOnMasterProcessingForDrainAsync(JobRawModel job)
    {
        try
        {
            job.MarkAsHeldOnMaster();
            await backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(job));
            return SaveDrainResultCode.Success;
        }
        catch (JobMasterVersionConflictException)
        {
            // Conflict means another runner already claimed or updated this job — treat as handled.
            return SaveDrainResultCode.Skipped;
        }
        catch
        {
            logger.Error("Failed to hold job on master", JobMasterLogCategory.Job, job.Id);
            return SaveDrainResultCode.Failed;
        }
    }
    
    /// <summary>
    /// Main ingestion path for a <c>PendingSave</c> job. Routes the job through one of three paths
    /// depending on its next-execution time, the local engine state, and bucket availability:
    /// <list type="number">
    ///   <item>
    ///     <description>
    ///       <b>HeldOnMaster (future job):</b> if <c>NextPlanExecutionAt</c> is beyond
    ///       <paramref name="cutOffDate"/> the job is inserted directly as <c>HeldOnMaster</c>
    ///       so the master scheduler can re-evaluate it later.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <b>Short-circuit into local engine:</b> if the job falls within the onboarding window,
    ///       the owning bucket is active, and the local <c>JobsExecutionEngine</c> has onboarding
    ///       capacity, the job is persisted and injected directly — skipping the NATS publish round-trip.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <b>Bucket selection + publish:</b> a target bucket is selected via
    ///       <see cref="IMasterBucketsService"/>, the job is assigned and persisted, then published
    ///       to the agent dispatcher for remote execution.
    ///     </description>
    ///   </item>
    /// </list>
    /// In all failure paths the job is held on master so it can be reassigned on the next cycle.
    /// </summary>
    /// <param name="jobRaw">The pending-save job to process.</param>
    /// <param name="cutOffDate">
    /// The horizon beyond which a job's next execution is considered too far in the future to
    /// dispatch now; jobs past this threshold are held on master.
    /// </param>
    /// <returns>
    /// An <see cref="AddSavePendingResult"/> describing the outcome code, the bucket the job was
    /// assigned to (if any), the published message ID (if dispatched), and any exception that
    /// occurred during a failed publish attempt.
    /// </returns>
    public async Task<AddSavePendingResult> AddSavePendingJobAsync(JobRawModel jobRaw, DateTime cutOffDate)
    {
        
        // Insert-first flow to avoid extra read; duplicate key maps to AlreadyExists
        if (jobRaw.GetSafeNextPlanExecutionAt() > cutOffDate)
        {
            jobRaw.MarkAsHeldOnMaster();
            try
            {
                await workerClusterOperations.ExecWithRetryAsync(o => o.AddAsync(jobRaw), millisecondsToDelay: 25);
            }
            catch (JobMasterDuplicationException ex)
            {
                logger.Warn("Job duplication detected", JobMasterLogCategory.Job, jobRaw.Id, exception: ex);
                return new AddSavePendingResult(AddSavePendingResultCode.AlreadyExists);
            }
            catch (Exception ex)
            {
                logger.Error("Failed to persist job as HeldOnMaster.", JobMasterLogCategory.Job, jobRaw.Id, exception: ex);
                throw;
            }
            
            return new AddSavePendingResult(AddSavePendingResultCode.HeldOnMaster);
        }

        var currentBucket = masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
        var engine = backgroundAgentWorker.GetEngine(bucketId);
        
        logger.Debug($"AddSavePendingJobAsync: JobId={jobRaw.Id} " +
                     $"IsOnBoarding={jobRaw.IsWithinOnboardingWindow()} " +
                     $"EngineAvailable={engine is not null} " +
                     $"BucketStatus={currentBucket?.Status} " +
                     $"OnBoardingAvailability={engine?.CountOnBoardingAvailability()} " +
                     $"NextPlanAt={jobRaw.NextPlanExecutionAt:O} " +
                     $"CutOffDate={cutOffDate:O}");
        
        // Short-circuit: Try to inject directly into JobsExecutionEngine if on same worker
        if (engine is not null && 
            jobRaw.Status == JobMasterJobStatus.PendingSave && 
            currentBucket?.Status == BucketStatus.Active &&
            jobRaw.IsWithinOnboardingWindow() && 
            engine.HasOnBoardingAvailability())
        {
            jobRaw.AssignToBucket(currentBucket);
            
            logger.Debug("Short-circuit: adding directly into the execution engine", JobMasterLogCategory.Job, jobRaw.Id);
            try
            {
                await workerClusterOperations.ExecWithRetryAsync(o => o.AddAsync(jobRaw), millisecondsToDelay: 25);
            }
            catch (JobMasterDuplicationException e)
            {
                logger.Warn("Job duplication detected", JobMasterLogCategory.Job, jobRaw.Id, exception: e);
                return new AddSavePendingResult(AddSavePendingResultCode.AlreadyExists);
            }

            try
            {
                // Force is safe here; capacity was already checked above.
                var result = await engine.TryOnBoardingJobAsync(jobRaw, forceIfNoCapacity: true);
                if (result == OnBoardingResult.Accepted)
                {
                    logger.Debug("Short-circuit accepted", JobMasterLogCategory.Job, jobRaw.Id);
                    return new AddSavePendingResult(AddSavePendingResultCode.Published, bucketId, null);
                }

                if (result == OnBoardingResult.MovedToMaster)
                {
                    return new AddSavePendingResult(AddSavePendingResultCode.HeldOnMaster, bucketId, null);
                }
                
                if (result == OnBoardingResult.Cancelled)
                {
                    // Engine already updated the job state (cancelled or failed by recurring schedule check).
                    return new AddSavePendingResult(AddSavePendingResultCode.HeldOnMaster);
                }

                // TooEarly, Busy, Invalid: job is persisted as InBucket but engine rejected it.
                // Hold on master so the master runner can reassign it on the next cycle.
                logger.Error($"Unexpected OnBoardingResult. JobId={jobRaw.Id} Result={result}", JobMasterLogCategory.Job, jobRaw.Id);
                jobRaw.MarkAsHeldOnMaster();
                await workerClusterOperations.ExecWithRetryAsync(o => o.UpsertAsync(jobRaw), millisecondsToDelay: 25);
                return new AddSavePendingResult(AddSavePendingResultCode.HeldOnMaster);
            }
            catch (Exception e)
            {
                logger.Error("Short-circuit failed to add job to processing", JobMasterLogCategory.Job, jobRaw.Id, exception: e);
                jobRaw.MarkAsHeldOnMaster();
                await workerClusterOperations.ExecWithRetryAsync(o => o.UpsertAsync(jobRaw), millisecondsToDelay: 25);
                return new AddSavePendingResult(AddSavePendingResultCode.PublishFailed, bucketId: bucketId, exception: e);
            }
        }

        var selectedBucket = await masterBucketsService.SelectBucketAsync(
            JobMasterConstants.BucketFastAllowDiscrepancy,
            jobRaw.Priority,
            jobRaw.WorkerLane);

        if (selectedBucket == null)
        {
            jobRaw.MarkAsHeldOnMaster();
            try
            {
                await workerClusterOperations.ExecWithRetryAsync(o => o.AddAsync(jobRaw), millisecondsToDelay: 25);
            }
            catch (JobMasterDuplicationException e)
            {
                logger.Warn("Job duplication detected", JobMasterLogCategory.Job, jobRaw.Id, exception: e);
                return new AddSavePendingResult(AddSavePendingResultCode.AlreadyExists);
            }
            
            return new AddSavePendingResult(AddSavePendingResultCode.HeldOnMasterNoBucket);
        }

        jobRaw.AssignToBucket(selectedBucket);
        try
        {
            await workerClusterOperations.ExecWithRetryAsync(o => o.AddAsync(jobRaw), millisecondsToDelay: 25);
        }
        catch (JobMasterDuplicationException e)
        {
            logger.Warn("Job duplication detected", JobMasterLogCategory.Job, jobRaw.Id, exception: e);
            return new AddSavePendingResult(AddSavePendingResultCode.AlreadyExists);
        }
        catch (Exception e)
        {
            logger.Error("Failed to insert job; holding on master", JobMasterLogCategory.Job, jobRaw.Id, exception: e);
            jobRaw.MarkAsHeldOnMaster();
            await workerClusterOperations.ExecWithRetryAsync(o => o.UpsertAsync(jobRaw), millisecondsToDelay: 25);
            return new AddSavePendingResult(AddSavePendingResultCode.PublishFailed, bucketId: selectedBucket.Id, exception: e);
        }

        try
        {
            logger.Debug($"Publishing job {jobRaw.Id} to agent {jobRaw.AgentWorkerId} bucket {selectedBucket.Id}", JobMasterLogCategory.Job, jobRaw.Id);
            
            var publishedMessageId = await agentJobsDispatcherService.AddForProcessingAsync(jobRaw);
            
            return new AddSavePendingResult(AddSavePendingResultCode.Published, bucketId: selectedBucket.Id, publishedMessageId: publishedMessageId);
        }
        catch (PublishOutcomeUnknownException ex)
        { 
            logger.Error("Publish outcome unknown for job; will hold on master", JobMasterLogCategory.Job, jobRaw.Id, exception: ex);
            jobRaw.MarkAsHeldOnMaster();
            await workerClusterOperations.ExecWithRetryAsync(o => o.UpsertAsync(jobRaw), millisecondsToDelay: 25);
            return new AddSavePendingResult(AddSavePendingResultCode.HeldOnMasterPublishedUnknown, bucketId: selectedBucket.Id, publishedMessageId: ex.SupposedPublishedId, exception: ex);
        }
        catch (Exception e)
        { 
            logger.Error("Failed to add job to processing", JobMasterLogCategory.Job, jobRaw.Id, exception: e);
            jobRaw.MarkAsHeldOnMaster();
            await workerClusterOperations.ExecWithRetryAsync(o => o.UpsertAsync(jobRaw), millisecondsToDelay: 25);
            return new AddSavePendingResult(AddSavePendingResultCode.PublishFailed, bucketId: selectedBucket.Id, exception: e);
        }
    }
}