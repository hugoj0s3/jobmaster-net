namespace JobMaster.Sdk.Abstractions;

/// <summary>
/// Defines system-wide constants for JobMaster infrastructure.
/// These values are not configurable and represent core system behavior.
/// </summary>
internal static class JobMasterConstants
{
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
    /// Cache freshness window for the agent workers list. Kept short because worker
    /// liveness read from it drives bucket assignment.
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

    /// <summary>
    /// Shared "is this resource still alive" window used by <c>AgentConnectionModel</c>,
    /// <c>AgentWorkerModel</c>, and <c>HostModel</c> — a 30-second heartbeat threshold plus
    /// <see cref="ClockSkewPadding"/> to tolerate clock drift between processes.
    /// </summary>
    public static readonly TimeSpan ResourceAliveThreshold = TimeSpan.FromSeconds(30).Add(ClockSkewPadding);
    
    public static readonly TimeSpan BucketNoJobsBeforeReadyToDelete = TimeSpan.FromMinutes(10);

    /// <summary>Max distinct agent connections dispatched to concurrently -- each is a separate DB/broker.</summary>
    public const int MaxParallelAgentConnectionDispatch = 10;

    /// <summary>Max buckets dispatched to concurrently within a single agent connection.</summary>
    public const int MaxParallelBucketDispatchPerAgentConnection = 5;
    /// <summary>
    /// Graceful stop (requested, e.g. by a signal): the worker takes no new jobs and runs the ones it has for this
    /// long, then stops immediately.
    /// </summary>
    public static readonly TimeSpan DefaultGracefulStopPeriod = TimeSpan.FromMinutes(15);

    /// <summary>The shortest graceful stop period a request may ask for.</summary>
    public static readonly TimeSpan MinGracefulStopPeriod = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Immediate stop (SIGTERM, process exit, end of a graceful stop): how long a running job gets before it is
    /// aborted and goes back to master. Sized for Docker's default 10 s before SIGKILL, leaving room for the cleanup.
    /// </summary>
    public static readonly TimeSpan ImmediateGracefulStopPeriod = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Immediate stop: the time after <see cref="ImmediateGracefulStopPeriod"/> for the cleanup (interrupted jobs
    /// written back to master, the flush of buffered jobs).
    /// </summary>
    public static readonly TimeSpan ImmediateStopCleanupPeriod = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Immediate stop, the whole budget (<see cref="ImmediateGracefulStopPeriod"/> +
    /// <see cref="ImmediateStopCleanupPeriod"/>): how long the runtime waits for each worker, and a task queue for its
    /// running jobs. Leaves room in Docker's default 10 s before SIGKILL for the loggers' final flush.
    /// </summary>
    public static readonly TimeSpan ImmediateStopTimeout = ImmediateGracefulStopPeriod + ImmediateStopCleanupPeriod;

    /// <summary>
    /// The default process deadline duration for jobs.
    /// Jobs must be processed within this time from their scheduled time or current time (whichever is later).
    /// </summary>
    public static readonly TimeSpan JobProcessDeadlineDefaultDuration = TimeSpan.FromMinutes(10);

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
    /// How long a bucket can stay Lost with no Full or Drain worker on its agent connection before it's logged as
    /// Critical instead of Warn: longer than a restart or deploy takes.
    /// </summary>
    public static readonly TimeSpan LostBucketNoWorkerGracePeriod = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Leases after which a save-pending message stops being retried: a job is saved on master as Failed, anything
    /// else is moved to the terminated bucket.
    /// </summary>
    public const int MaxSavePendingLeaseCount = 1000;

    /// <summary>
    /// Default TTL for sentinel-backed cache entries. Sentinel invalidation handles freshness;
    /// this is just a safety-net expiry to prevent stale entries if a notification is ever missed.
    /// </summary>
    public static readonly TimeSpan DefaultCacheEntryExpiry = TimeSpan.FromHours(8);

    public static DateTime NowUtcWithSkewTolerance()
    {
        return DateTime.UtcNow.Add(-ClockSkewPadding);
    }
    
    /// <summary>
    /// Prefix shared by every agent connection name reserved by the framework itself. Chosen to be
    /// distinctive enough that a user is unlikely to pick it by accident — any name using this prefix
    /// is treated as reserved (see <see cref="IsReservedAgentConnectionName"/>), so future reserved
    /// names are automatically covered without updating that check.
    /// </summary>
    public const string ReservedAgentConnectionNamePrefix = "JMReserved-";

    public const string StandaloneAgentConnName = ReservedAgentConnectionNamePrefix + "standalone";

    public const string MasterFallbackAgentConnName = ReservedAgentConnectionNamePrefix + "fallback";

    public static bool IsReservedAgentConnectionName(string? name) =>
        name is not null && name.StartsWith(ReservedAgentConnectionNamePrefix, StringComparison.OrdinalIgnoreCase);
}
