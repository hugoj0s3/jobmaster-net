namespace JobMaster.Abstractions.Models;

/// <summary>
/// Represents the lifecycle state of a bucket — the unit of job ownership assigned to a
/// background agent worker.
/// </summary>
public enum BucketStatus
{
    /// <summary> 
    /// The bucket is healthy and fully operational. The bucket is ready to accept new jobs.
    /// </summary>
    Active = 1,

    /// <summary>
    /// The bucket is winding down. All jobs that are not enqueued or executing will be returned to master.
    /// </summary>
    Completing = 2,

    /// <summary>
    /// All in-flight jobs have been returned to master and the engine is idle. The bucket
    /// is waiting for a drain runner to pick it up and process any remaining
    /// <c>HeldOnMaster</c> jobs.
    /// </summary>
    ReadyToDrain = 3,

    /// <summary>
    /// A drain runner has claimed the bucket and is actively moving jobs to <c>HeldOnMaster</c>
    /// Once it is HeldOnMaster the jobs can be re-assigned to a active bucket.
    /// </summary>
    Draining = 4,

    /// <summary>
    /// The worker that owned this bucket terminated unexpectedly. The bucket is orphaned
    /// and awaiting to be drained.
    /// </summary>
    Lost = 5,

    /// <summary>
    /// All jobs have either completed or been drained. The bucket is fully empty and
    /// waiting for the final deletion confirmation before being removed from the store.
    /// </summary>
    ReadyToDelete = 6,
}