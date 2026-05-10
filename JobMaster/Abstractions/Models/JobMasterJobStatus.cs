namespace JobMaster.Abstractions.Models;

/// <summary>
/// Represents the lifecycle state of a job as it moves through the JobMaster pipeline.
/// </summary>
/// <remarks>
/// Flow: <c>PendingSave</c> → <c>OnMaster</c> → <c>InBucket</c> → <c>Onboarded</c> → <c>Queued</c> → <c>Processing</c> → <c>Succeeded</c>
/// When a job is scheduled within the transient threshold window, it skips <c>OnMaster</c>
/// and is assigned directly to <c>InBucket</c>.
/// </remarks>
public enum JobMasterJobStatus
{
    /// <summary>
    /// The job has been created by the application but has not yet been persisted to the
    /// master store. It is temporarily held in the agent's transport layer until it is
    /// stored to the master db, usually less than 2 or 3 seconds.
    /// </summary>
    PendingSave = 1,

    /// <summary>
    /// The job is persisted in the master store and waiting to be assigned to a bucket.
    /// When TransientThreshold is reached, the job is moved to <c>InBucket</c>.
    /// </summary>
    OnMaster = 2,

    /// <summary>
    /// The job has been assigned/stored to a specific bucket according to the TransientThreshold window.
    /// Once the job schedules execution time, it will move to <c>Onboarded</c>.
    /// </summary>
    InBucket = 3,

    /// <summary>
    /// The job has been accepted by the worker and is held in memory, very close to being
    /// queued. It typically enters this state around 30 seconds before the scheduled
    /// execution time.
    /// </summary>
    Onboarded = 10,

    /// <summary>
    /// The job is waiting for a free execution slot on the worker.
    /// </summary>
    Queued = 6,

    /// <summary>
    /// The job is actively being executed by its registered <c>IJobHandler</c>. Execution
    /// metadata (<c>JobExecution</c>) is created and persisted when this state is entered.
    /// </summary>
    Processing = 4,

    /// <summary>
    /// The job completed successfully. This is a final state — the job will not be
    /// retried or reassigned.
    /// </summary>
    Succeeded = 5,

    /// <summary>
    /// The job failed and all configured retries have been exhausted. This is a final
    /// state. Use <see cref="Aborted"/> for jobs that were forcibly stopped rather than
    /// failing through normal execution.
    /// </summary>
    Failed = 7,

    /// <summary>
    /// The job was explicitly cancelled before typically because
    /// the associated recurring schedule was terminated or by a user.
    /// </summary>
    Cancelled = 8,

    /// <summary>
    /// The job was forcibly stopped forcibly by a user or system.
    /// </summary>
    Aborted = 9,
}

public static class JobMasterJobStatusUtil
{
    public static bool IsFinalStatus(this JobMasterJobStatus jobStatus) => GetFinalStatuses().Contains(jobStatus);
    
    public static bool IsBucketStatus(this JobMasterJobStatus jobStatus) => GetBucketStatuses().Contains(jobStatus);
    
    public static IList<JobMasterJobStatus> GetFinalStatuses() => 
        new List<JobMasterJobStatus>
        {
            JobMasterJobStatus.Succeeded, 
            JobMasterJobStatus.Failed, 
            JobMasterJobStatus.Cancelled, 
            JobMasterJobStatus.Aborted
        };
    
    public static IList<JobMasterJobStatus> GetBucketStatuses() => 
        new List<JobMasterJobStatus> { JobMasterJobStatus.InBucket, JobMasterJobStatus.Onboarded, JobMasterJobStatus.Queued };
}