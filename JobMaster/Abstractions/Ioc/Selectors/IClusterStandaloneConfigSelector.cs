using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Abstractions.Ioc.Selectors;

/// <summary>
/// Fluent configuration for a cluster running in standalone mode.
/// In standalone mode the master scheduler and the worker run in the same process —
/// no separate agent connection or messaging infrastructure is required.
/// All methods return the same selector instance to allow method chaining.
/// </summary>
public interface IClusterStandaloneConfigSelector
{
    /// <summary>
    /// Marks this cluster as the default cluster.
    /// When multiple clusters are registered, the default is used whenever no explicit cluster ID is specified.
    /// </summary>
    public IClusterStandaloneConfigSelector SetAsDefault();

    /// <summary>
    /// Sets the unique identifier for this cluster.
    /// This ID is used to route jobs and distinguish clusters in a multi-cluster setup.
    /// </summary>
    /// <param name="clusterId">A unique string that identifies this cluster.</param>
    public IClusterStandaloneConfigSelector ClusterId(string clusterId);

    /// <summary>
    /// Sets the default maximum execution time allowed for a single job.
    /// Jobs that run longer than this threshold are considered timed out and marked as failed.
    /// Default: <see cref="JobMasterDefaults.DefaultJobTimeout"/> (1 minute).
    /// </summary>
    /// <param name="defaultJobTimeout">The maximum allowed execution duration per job.</param>
    public IClusterStandaloneConfigSelector ClusterDefaultJobTimeout(TimeSpan defaultJobTimeout);

    /// <summary>
    /// Sets the look-ahead window within which jobs are dispatched from the Master to a bucket.
    /// Jobs scheduled beyond this threshold remain held on the Master until they fall within the window.
    /// Default: <see cref="JobMasterDefaults.TransientThreshold"/> (10 minutes).
    /// </summary>
    /// <param name="transientThreshold">The look-ahead dispatch window.</param>
    public IClusterStandaloneConfigSelector ClusterTransientThreshold(TimeSpan transientThreshold);

    /// <summary>
    /// Sets the maximum number of automatic retries for a failed job before it is permanently marked as failed.
    /// Default: <see cref="JobMasterDefaults.MaxRetryCount"/> (3 retries).
    /// </summary>
    /// <param name="defaultMaxRetryCount">The number of retry attempts allowed per job.</param>
    public IClusterStandaloneConfigSelector ClusterDefaultMaxRetryCount(int defaultMaxRetryCount);

    /// <summary>
    /// Sets the maximum byte size for job messages exchanged through the internal pipeline.
    /// Jobs whose payload exceeds this limit will be rejected at submission time.
    /// Use -1 to disable the limit (not recommended in constrained environments).
    /// Default: <see cref="JobMasterDefaults.MaxMessageByteSize"/> (128 KB).
    /// </summary>
    /// <param name="maxMessageByteSize">The maximum message size in bytes.</param>
    public IClusterStandaloneConfigSelector ClusterMaxMessageByteSize(int maxMessageByteSize);

    /// <summary>
    /// Sets the IANA time zone ID used when evaluating recurring job schedules (e.g. cron expressions).
    /// Example values: "America/New_York", "Europe/London", "UTC".
    /// Default: the local system time zone.
    /// </summary>
    /// <param name="ianaTimeZoneId">A valid IANA time zone identifier.</param>
    public IClusterStandaloneConfigSelector ClusterIanaTimeZoneId(string ianaTimeZoneId);

    /// <summary>
    /// Sets the cluster-wide data retention window (TTL) for executed jobs, inactive recurring
    /// schedules, and JobMaster logs, keeping dashboards consistent (e.g. a failed job remains
    /// alongside its logs until both are purged together).
    /// Default: <see cref="JobMasterDefaults.DataRetentionTtl"/> (infinite — no automatic purge).
    /// </summary>
    /// <param name="dataRetentionTtl">
    /// The retention window. Must be ≥ <see cref="JobMasterDefaults.MinDataRetentionTtl"/> (10 minutes) when positive.
    /// Zero or negative is accepted as infinite retention (equivalent to <see cref="RetainDataForever"/>).
    /// Throws <see cref="ArgumentException"/> for positive values under 10 minutes.
    /// </param>
    public IClusterStandaloneConfigSelector ClusterDataRetentionTtl(TimeSpan dataRetentionTtl);

    /// <summary>
    /// Disables automatic data purging for this cluster. Executed jobs, inactive recurring schedules,
    /// and JobMaster logs are kept indefinitely.
    /// </summary>
    public IClusterStandaloneConfigSelector RetainDataForever();

    /// <summary>
    /// Disables the specified priority level for this cluster.
    /// Once disabled: no execution bucket is created for this priority at startup;
    /// any job handler or static recurring schedule configured for this priority throws at startup;
    /// and scheduling a job at this priority throws at scheduling time.
    /// <see cref="JobMasterPriority.Medium"/> cannot be disabled — it is the fallback for all
    /// handlers that do not declare an explicit <see cref="JobMasterPriority"/> attribute.
    /// </summary>
    /// <param name="priority">The priority to disable. Must not be <see cref="JobMasterPriority.Medium"/>.</param>
    public IClusterStandaloneConfigSelector DisablePriority(JobMasterPriority priority);

    /// <summary>
    /// Registers an in-process worker that picks up and executes jobs for this standalone cluster.
    /// </summary>
    /// <param name="workerName">Optional logical name for this worker instance.</param>
    /// <param name="batchSize">Number of jobs pulled per transfer cycle. Default: 250.</param>
    public IClusterStandaloneConfigSelector AddWorker(string? workerName = null, int batchSize = 250);

    /// <summary>
    /// Sets the operational mode of this cluster.
    /// Default: <see cref="JobMasterDefaults.DefaultClusterMode"/> (<see cref="ClusterMode.Active"/>).
    /// See <see cref="ClusterMode"/> for a description of each mode.
    /// </summary>
    /// <param name="mode">The desired cluster mode.</param>
    public IClusterStandaloneConfigSelector ClusterMode(ClusterMode mode);

    internal IClusterStandaloneConfigSelector ClusterConnString(string connString);
    internal IClusterStandaloneConfigSelector ClusterRepoType(string repoType);

    internal IClusterStandaloneConfigSelector ClusterAdditionalConfig(JobMasterConfigDictionary additionalConfig);
}
