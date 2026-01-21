namespace JobMaster.Sdk.Abstractions;

/// <summary>
/// Defines system-wide constants for JobMaster infrastructure.
/// These values are not configurable and represent core system behavior.
/// </summary>
public static class JobMasterConstants
{
    /// <summary>
    /// The maximum time allowed between worker heartbeats before a worker is considered dead.
    /// Workers must send heartbeats more frequently than this threshold to be considered alive.
    /// </summary>
    public static readonly TimeSpan HeartbeatThreshold = TimeSpan.FromSeconds(30);
    
    /// <summary>
    /// The grace period added to HeartbeatThreshold before permanently cleaning up a dead worker.
    /// This prevents premature cleanup of workers that might be temporarily unresponsive.
    /// </summary>
    public static readonly TimeSpan DeadWorkerCleanupGracePeriod = TimeSpan.FromMinutes(30);

    public static readonly TimeSpan MaxRunnerInterval = TimeSpan.FromMinutes(10);
    
    public static readonly TimeSpan MaxAllowedDiscrepancy = TimeSpan.FromMinutes(5);
    
    public static readonly TimeSpan BucketFastAllowDiscrepancy = TimeSpan.FromSeconds(10);
    
    public static readonly TimeSpan BucketDefaultAllowDiscrepancy = TimeSpan.FromMinutes(2.5);
    
    public static readonly TimeSpan MinBucketStatusTransitionInterval = BucketFastAllowDiscrepancy.Add(TimeSpan.FromSeconds(5));
    
    public static readonly TimeSpan DurationToLockRecords = TimeSpan.FromMinutes(5);

    public static readonly TimeSpan ClockSkewPadding = TimeSpan.FromSeconds(15);
    
    public static readonly TimeSpan BucketNoJobsBeforeReadyToDelete = TimeSpan.FromMinutes(30);

    public const int MaxBatchSizeForBulkOperation = 50;
    public static readonly TimeSpan DefaultGracefulStopPeriod = TimeSpan.FromMinutes(15);
    
    /// <summary>
    /// The default process deadline duration for jobs.
    /// Jobs must be processed within this time from their scheduled time or current time (whichever is later).
    /// </summary>
    public static readonly TimeSpan JobProcessDeadlineDuration = TimeSpan.FromMinutes(10);
    public static readonly TimeSpan OnBoardingWindow = TimeSpan.FromSeconds(30);

    public static DateTime NowUtcWithSkewTolerance()
    {
        return DateTime.UtcNow.Add(-ClockSkewPadding);
    }
}
