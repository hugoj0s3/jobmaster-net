using JobMaster.Abstractions.Models;

namespace JobMaster.Abstractions;

/// <summary>
/// Central source of truth for all JobMaster default configuration values.
/// Referenced by model initializers and selector XML documentation to keep
/// defaults consistent and easy to maintain.
/// </summary>
public static class JobMasterDefaults
{
    /// <summary>Default maximum execution time per job: 1 minute.</summary>
    public static readonly TimeSpan DefaultJobTimeout = TimeSpan.FromMinutes(1);

    /// <summary>Default window before a dispatched-but-unacknowledged job is requeued: 10 minutes.</summary>
    public static readonly TimeSpan TransientThreshold = TimeSpan.FromMinutes(10);

    /// <summary>Default maximum number of automatic retries per failed job: 3.</summary>
    public const int MaxRetryCount = 3;

    /// <summary>Default maximum job message size: 128 KB (131,072 bytes).</summary>
    public const int MaxMessageByteSize = 128 * 1024;

    /// <summary>Default cluster operational mode: <see cref="ClusterMode.Active"/>.</summary>
    public const ClusterMode DefaultClusterMode = ClusterMode.Active;

    /// <summary>Default data retention window: infinite (no automatic purge).</summary>
    public static readonly TimeSpan DataRetentionTtl = TimeSpan.Zero;

    /// <summary>Minimum accepted positive data retention TTL: 10 minutes. Cleanup runners check at TTL/2, so the minimum check interval is 5 minutes.</summary>
    public static readonly TimeSpan MinDataRetentionTtl = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Default configuration values for individual workers.
    /// </summary>
    public static class Worker
    {
        /// <summary>Default number of jobs pulled from the master per transfer cycle: 1000.</summary>
        public const int TransferBatchSize = 1000;

        /// <summary>Default local in-memory bucket buffer capacity: 250.</summary>
        public const int BucketBufferSize = 250;

        /// <summary>Default look-ahead window for pre-fetching job buckets: 15 seconds.</summary>
        public static readonly TimeSpan BucketBufferLeadTime = TimeSpan.FromSeconds(15);

        /// <summary>Default parallelism multiplier: 1.0 (no scaling).</summary>
        public const double ParallelismFactor = 1.0;

        /// <summary>Default concurrent execution slots per priority level: 1.</summary>
        public const int BucketQtyPerPriority = 1;

        /// <summary>Default worker operational mode: <see cref="AgentWorkerMode.Full"/>.</summary>
        public const AgentWorkerMode DefaultMode = AgentWorkerMode.Full;
    }
}
