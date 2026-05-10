using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Sdk.Abstractions.Background;

/// <summary>
/// Controls the full job-execution lifecycle within a single bucket: staging, onboarding,
/// concurrent task queuing, and graceful shutdown. One engine instance exists per one active bucket.
/// </summary>
internal interface IJobsExecutionEngine
{
    /// <summary>
    /// Holds jobs that have been admitted for onboarding and are waiting for their
    /// departure time before moving to the task queue.
    /// </summary>
    IOnBoardingControl<JobRawModel> OnBoardingControl { get; }

    /// <summary>
    /// Manages the pool of concurrently running and waiting-to-run job tasks for this bucket.
    /// </summary>
    ITaskQueueControl<JobRawModel> TaskQueueControl { get; }

    /// <summary>The identifier of the bucket this engine is bound to.</summary>
    string BucketId { get; }

    /// <summary>
    /// Returns <see langword="true"/> when there is room to accept at least one more job for onboarding.
    /// </summary>
    bool HasOnBoardingAvailability();

    /// <summary>
    /// Returns the number of additional jobs this engine can accept for onboarding.
    /// Accounts for jobs already staged in the flush buffer but not yet pushed to
    /// <see cref="OnBoardingControl"/>.
    /// </summary>
    int CountOnBoardingAvailability();

    /// <summary>
    /// Attempts to accept a job into the onboarding staging buffer.
    /// Validates the recurring schedule (if applicable) before admission and returns a
    /// result describing whether the job was accepted, deferred, cancelled, or moved back to master.
    /// </summary>
    /// <param name="payload">The job to onboard.</param>
    /// <param name="forceIfNoCapacity">
    /// When <see langword="true"/>, bypasses the capacity check and stages the job regardless
    /// of current buffer fullness. Used during rebalancing to avoid dropping jobs.
    /// </param>
    Task<OnBoardingResult> TryOnBoardingJobAsync(JobRawModel payload, bool forceIfNoCapacity = false);

    /// <summary>
    /// Graceful-shutdown entry point. Collects all jobs still held in the onboarding buffer,
    /// the task waiting queue, and the staging list, marks them as
    /// <see cref="JobMasterJobStatus.OnMaster"/>, and bulk-updates the master so they can be
    /// recovered by the next available bucket. Errors during bulk update are logged but not
    /// rethrown so the caller's shutdown sequence is never blocked.
    /// </summary>
    Task FlushToMasterAsync();

    /// <summary>
    /// Drives the engine's recurring work: flushes the staging buffer into
    /// <see cref="OnBoardingControl"/>, starts queued tasks when slots are free,
    /// drains the bucket if it is completing, and enqueues ready jobs for execution.
    /// Called by the background runner on every tick.
    /// </summary>
    Task PulseAsync();
}