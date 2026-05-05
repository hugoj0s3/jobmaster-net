namespace JobMaster.Sdk.Abstractions;

/// <summary>
/// Defines system-wide constants for JobMaster infrastructure.
/// These values are not configurable and represent core system behavior.
/// </summary>
internal static class JobMasterConstants
{
    /// <summary>
    /// The maximum time allowed between worker heartbeats before a worker is considered dead.
    /// Workers must send heartbeats more frequently than this threshold to be considered alive.
    /// </summary>
    public static readonly TimeSpan AgentHeartbeatThreshold = TimeSpan.FromSeconds(30);
    
    /// <summary>
    /// The grace period added to HeartbeatThreshold before permanently cleaning up a dead worker.
    /// This prevents premature cleanup of workers that might be temporarily unresponsive.
    /// </summary>
    public static readonly TimeSpan DeadWorkerCleanupGracePeriod = TimeSpan.FromMinutes(30);

    public static readonly TimeSpan MaxRunnerInterval = TimeSpan.FromMinutes(10);
    
    public static readonly TimeSpan MaxAllowedDiscrepancy = TimeSpan.FromMinutes(5);
    
    public static readonly TimeSpan BucketFastAllowDiscrepancy = TimeSpan.FromSeconds(10);

    public static readonly TimeSpan BucketDefaultAllowDiscrepancy = TimeSpan.FromMinutes(2.5);

    /// <summary>
    /// Cache freshness window for the hosts list. Host data is purely informational
    /// (display, stats), so a longer window is acceptable in exchange for cache effectiveness.
    /// </summary>
    public static readonly TimeSpan HostsAllowDiscrepancy = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Cache freshness window for the agent workers list. Kept short because
    /// <c>StopRequestedAt</c> flows from this cache through to each agent's stop-graceful
    /// path — stale reads here directly delay graceful shutdown.
    /// </summary>
    public static readonly TimeSpan AgentWorkersAllowDiscrepancy = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Cache freshness window for cluster configuration. Configuration changes are rare,
    /// so a longer window is fine, but capped at 2 minutes so operator-driven config
    /// rollouts converge in a bounded time without manual cache busts.
    /// </summary>
    public static readonly TimeSpan ClusterConfigurationAllowDiscrepancy = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Cache freshness window for the agent connections list. Kept short because
    /// connections gate dispatch and worker registration paths.
    /// </summary>
    public static readonly TimeSpan AgentConnectionsAllowDiscrepancy = TimeSpan.FromSeconds(10);
    
    public static readonly TimeSpan MinBucketStatusTransitionInterval = BucketFastAllowDiscrepancy.Add(TimeSpan.FromSeconds(5));
    
    public static readonly TimeSpan DurationToLockRecords = TimeSpan.FromMinutes(5);

    public static readonly TimeSpan ClockSkewPadding = TimeSpan.FromSeconds(15);
    public static readonly TimeSpan SentinelNotifyPadding = TimeSpan.FromSeconds(1);
    
    public static readonly TimeSpan BucketNoJobsBeforeReadyToDelete = TimeSpan.FromMinutes(10);

    public const int MaxBatchSizeForBulkOperation = 50;
    public const int MaxAllowedRetries = 10;
    public static readonly TimeSpan DefaultGracefulStopPeriod = TimeSpan.FromMinutes(15);
    
    /// <summary>
    /// The default process deadline duration for jobs.
    /// Jobs must be processed within this time from their scheduled time or current time (whichever is later).
    /// </summary>
    public static readonly TimeSpan JobProcessDeadlineDuration = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Combined early-warning buffer used when evaluating whether a job's ProcessDeadline is approaching.
    /// Incorporates <see cref="ClockSkewPadding"/> plus a 1-minute lead time so callers detect expiry
    /// before <c>HeldOnMasterDeadlineTimeoutJobsRunner</c> acquires the job, avoiding version conflicts.
    /// </summary>
    public static readonly TimeSpan ProcessDeadlineEarlyWarning = ClockSkewPadding.Add(TimeSpan.FromMinutes(1));
    public static readonly TimeSpan OnBoardingWindow = TimeSpan.FromSeconds(30);
    
    /// <summary>
    /// The minimum delay before retrying job execution when the onboarding list is busy/saturated.
    /// This ensures jobs are not retried too aggressively when workers cannot accept new jobs.
    /// </summary>
    public static readonly TimeSpan MinDelayWhenOnboardingBusy = TimeSpan.FromSeconds(15);

    public static readonly TimeSpan NoBucketFallbackThreshold = TimeSpan.FromMinutes(2.5);

    /// <summary>
    /// Default TTL for sentinel-backed cache entries. Sentinel invalidation handles freshness;
    /// this is just a safety-net expiry to prevent stale entries if a notification is ever missed.
    /// </summary>
    public static readonly TimeSpan DefaultCacheEntryExpiry = TimeSpan.FromHours(8);

    public static DateTime NowUtcWithSkewTolerance()
    {
        return DateTime.UtcNow.Add(-ClockSkewPadding);
    }
    
    public const string StandaloneAgentConnName = "standalone-agent-conn";
}
