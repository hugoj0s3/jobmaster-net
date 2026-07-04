using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Logs;
using Microsoft.Extensions.Configuration;

namespace JobMaster.Abstractions.Ioc.Selectors;

/// <summary>
/// Fluent configuration entry point for a JobMaster cluster.
/// Use this selector during DI setup to define the cluster identity, behavior limits,
/// messaging settings, workers, and agent connections.
/// All methods return the same selector instance to allow method chaining.
/// </summary>
public interface IClusterConfigSelector
{
    /// <summary>
    /// Marks this cluster as the default cluster.
    /// When multiple clusters are registered, the default is used whenever no explicit cluster ID is specified.
    /// </summary>
    public IClusterConfigSelector SetAsDefault();

    /// <summary>
    /// Sets the unique identifier for this cluster.
    /// This ID is used to route jobs and distinguish clusters in a multi-cluster setup.
    /// </summary>
    /// <param name="clusterId">A unique string that identifies this cluster.</param>
    public IClusterConfigSelector ClusterId(string clusterId);

    /// <summary>
    /// Sets the default maximum execution time allowed for a single job.
    /// Jobs that run longer than this threshold are considered timed out and marked as failed.
    /// Default: <see cref="JobMasterDefaults.DefaultJobTimeout"/> (1 minute).
    /// </summary>
    /// <param name="defaultJobTimeout">The maximum allowed execution duration per job.</param>
    public IClusterConfigSelector DefaultJobTimeout(TimeSpan defaultJobTimeout);

    /// <inheritdoc cref="DefaultJobTimeout"/>
    [Obsolete("Use DefaultJobTimeout instead.")]
    public IClusterConfigSelector ClusterDefaultJobTimeout(TimeSpan defaultJobTimeout);

    /// <summary>
    /// Sets the look-ahead window within which jobs are dispatched from the Master to a bucket.
    /// Jobs scheduled beyond this threshold remain held on the Master until they fall within the window.
    /// Default: <see cref="JobMasterDefaults.TransientThreshold"/> (10 minutes).
    /// </summary>
    /// <param name="transientThreshold">The look-ahead dispatch window.</param>
    public IClusterConfigSelector TransientThreshold(TimeSpan transientThreshold);

    /// <inheritdoc cref="TransientThreshold"/>
    [Obsolete("Use TransientThreshold instead.")]
    public IClusterConfigSelector ClusterTransientThreshold(TimeSpan transientThreshold);

    /// <summary>
    /// Sets the maximum number of automatic retries for a failed job before it is permanently marked as failed.
    /// Default: <see cref="JobMasterDefaults.MaxRetryCount"/> (3 retries).
    /// </summary>
    /// <param name="defaultMaxRetryCount">The number of retry attempts allowed per job.</param>
    public IClusterConfigSelector DefaultMaxRetryCount(int defaultMaxRetryCount);

    /// <inheritdoc cref="DefaultMaxRetryCount"/>
    [Obsolete("Use DefaultMaxRetryCount instead.")]
    public IClusterConfigSelector ClusterDefaultMaxRetryCount(int defaultMaxRetryCount);

    /// <summary>
    /// Sets the maximum byte size for job messages exchanged through the messaging layer.
    /// Jobs whose payload exceeds this limit will be rejected at submission time.
    /// Use -1 to disable the limit (not recommended in constrained environments).
    /// Default: <see cref="JobMasterDefaults.MaxMessageByteSize"/> (128 KB).
    /// </summary>
    /// <param name="maxMessageByteSize">The maximum message size in bytes.</param>
    public IClusterConfigSelector MaxMessageByteSize(int maxMessageByteSize);

    /// <inheritdoc cref="MaxMessageByteSize"/>
    [Obsolete("Use MaxMessageByteSize instead.")]
    public IClusterConfigSelector ClusterMaxMessageByteSize(int maxMessageByteSize);

    /// <summary>
    /// Sets the IANA time zone ID used when evaluating recurring job schedules (e.g. cron expressions).
    /// Example values: "America/New_York", "Europe/London", "UTC".
    /// Default: the local system time zone.
    /// </summary>
    /// <param name="ianaTimeZoneId">A valid IANA time zone identifier.</param>
    public IClusterConfigSelector IanaTimeZoneId(string ianaTimeZoneId);

    /// <inheritdoc cref="IanaTimeZoneId"/>
    [Obsolete("Use IanaTimeZoneId instead.")]
    public IClusterConfigSelector ClusterIanaTimeZoneId(string ianaTimeZoneId);

    /// <summary>
    /// Sets the cluster-wide data retention window (TTL) for executed jobs, inactive recurring
    /// schedules, and JobMaster logs, keeping dashboards consistent (e.g. a failed job remains
    /// alongside its logs until both are purged together).
    /// Default: <see cref="JobMasterDefaults.DataRetentionTtl"/> (infinite — no automatic purge).
    /// </summary>
    /// <param name="dataRetentionTtl">
    /// The retention window. Must be ≥ <see cref="JobMasterDefaults.MinDataRetentionTtl"/> (5 minutes) when positive.
    /// Zero or negative is accepted as infinite retention (equivalent to <see cref="RetainDataForever"/>).
    /// Throws <see cref="ArgumentException"/> for positive values under 1 hour.
    /// </param>
    public IClusterConfigSelector DataRetentionTtl(TimeSpan dataRetentionTtl);

    /// <summary>
    /// Disables automatic data purging for this cluster. Executed jobs, inactive recurring schedules,
    /// and JobMaster logs are kept indefinitely.
    /// </summary>
    public IClusterConfigSelector RetainDataForever();

    /// <summary>
    /// Registers a worker that picks up and executes jobs for this cluster.
    /// </summary>
    /// <param name="workerName">Optional logical name for this worker instance.</param>
    /// <param name="agentConnectionName">The name of the agent connection this worker belongs to. Required when more than one agent connection is registered.</param>
    /// <param name="transferBatchSize">
    /// Number of jobs pulled from the master DB per transfer cycle.
    /// Higher values reduce round-trip overhead at the cost of more memory per cycle.
    /// Default: <see cref="JobMasterDefaults.Worker.TransferBatchSize"/> (1000).
    /// </param>
    /// <param name="bucketBufferSize">
    /// Maximum number of job buckets held in the worker's local in-memory buffer.
    /// A larger buffer keeps the worker busier during bursts but uses more memory.
    /// Default: <see cref="JobMasterDefaults.Worker.BucketBufferSize"/> (250).
    /// </param>
    public IAgentWorkerSelector AddWorker(string? workerName = null, string? agentConnectionName = null, int transferBatchSize = 1000, int bucketBufferSize = 250);

    /// <summary>
    /// Registers an agent connection that allows this cluster to communicate with a remote worker node.
    /// The repository type and connection string can be provided directly or configured via the returned selector.
    /// </summary>
    /// <param name="agentConnectionName">The logical name for this agent connection.</param>
    /// <param name="repoType">Optional repository type identifier (e.g. the database driver to use).</param>
    /// <param name="cnnString">Optional connection string for the agent's data store.</param>
    public IAgentConnectionConfigSelector AddAgentConnectionConfig(
        string agentConnectionName,
        string? repoType = null,
        string? cnnString = null);

    /// <summary>
    /// Sets the operational mode of this cluster.
    /// Default: <see cref="JobMasterDefaults.DefaultClusterMode"/> (<see cref="ClusterMode.Active"/>).
    /// See <see cref="ClusterMode"/> for a description of each mode.
    /// </summary>
    /// <param name="mode">The desired cluster mode.</param>
    public IClusterConfigSelector Mode(ClusterMode mode);

    /// <inheritdoc cref="Mode"/>
    [Obsolete("Use Mode instead.")]
    public IClusterConfigSelector ClusterMode(ClusterMode mode);

    /// <summary>
    /// Switches this cluster to standalone mode, where the master and worker run in the same process.
    /// In standalone mode no separate agent connection is needed.
    /// Returns a selector to apply standalone-specific settings.
    /// </summary>
    public IClusterStandaloneConfigSelector UseStandaloneCluster();

    /// <summary>
    /// Bootstraps the full cluster configuration from an <see cref="IConfiguration"/> section,
    /// mirroring what the fluent API exposes. The section should match the cluster JSON schema.
    /// </summary>
    /// <param name="section">The configuration section to bind (e.g. <c>builder.Configuration.GetSection("JobMaster:Cluster")</c>).</param>
    public IClusterConfigSelector ConfigFromJson(IConfiguration section);

    /// <summary>
    /// Bootstraps the full cluster configuration from a raw JSON string or a JSON file path.
    /// If the value ends with <c>.json</c> or resolves to an existing file it is loaded from disk;
    /// otherwise the value is treated as a raw JSON string.
    /// </summary>
    /// <param name="jsonOrFilePath">A raw JSON string or an absolute/relative path to a <c>.json</c> file.</param>
    public IClusterConfigSelector ConfigFromJson(string jsonOrFilePath);

    /// <summary>
    /// Bootstraps the full cluster configuration by deserializing a JSON stream.
    /// Useful for loading configuration from embedded resources or other <see cref="Stream"/> sources.
    /// </summary>
    /// <param name="stream">A readable stream containing JSON that matches the cluster JSON schema.</param>
    public IClusterConfigSelector ConfigFromJson(Stream stream);

    internal IAgentConnectionConfigSelector AddAgentConnectionConfig(
        string agentConnectionName,
        string? repoType,
        string? cnnString,
        JobMasterConfigDictionary? additionalConnConfig);

    /// <summary>
    /// Enables a debug JSONL file logger that appends structured log entries to a local file.
    /// Useful for diagnosing cluster behaviour during development.
    /// </summary>
    /// <param name="filePath">The absolute or relative path of the output file.</param>
    /// <param name="maxBufferItems">Maximum number of log items to buffer before flushing. Default: 500.</param>
    /// <param name="flushInterval">How often the buffer is flushed to disk. Defaults to a reasonable internal interval when null.</param>
    public IClusterConfigSelector DebugJsonlFileLogger(string filePath, int maxBufferItems = 500, TimeSpan? flushInterval = null);

    internal IClusterConfigSelector ClusterAdditionalConfig(JobMasterConfigDictionary additionalConfig);

    internal IClusterConfigSelector ClusterAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig);

    internal IClusterConfigSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);
    internal IClusterConfigSelector AppendAdditionalConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);

    internal IClusterConfigSelector ClusterRuntimeDbOperationLimit(int runtimeDbOperationThrottleLimit);

    internal IClusterConfigSelector ClusterRepoType(string repoType);
    internal IClusterConfigSelector ClusterConnString(string connString);

    internal IClusterConfigSelector EnableMirrorLog(Action<LogItem> mirrorLog);

    internal void Finish();
}
