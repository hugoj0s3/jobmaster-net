using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Logs;

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
    public IClusterConfigSelector ClusterDefaultJobTimeout(TimeSpan defaultJobTimeout);

    /// <summary>
    /// Sets the look-ahead window within which jobs are dispatched from the Master to a bucket.
    /// Jobs scheduled beyond this threshold remain held on the Master until they fall within the window.
    /// Default: <see cref="JobMasterDefaults.TransientThreshold"/> (10 minutes).
    /// </summary>
    /// <param name="transientThreshold">The look-ahead dispatch window.</param>
    public IClusterConfigSelector ClusterTransientThreshold(TimeSpan transientThreshold);

    /// <summary>
    /// Sets the maximum number of automatic retries for a failed job before it is permanently marked as failed.
    /// Default: <see cref="JobMasterDefaults.MaxRetryCount"/> (3 retries).
    /// </summary>
    /// <param name="defaultMaxRetryCount">The number of retry attempts allowed per job.</param>
    public IClusterConfigSelector ClusterDefaultMaxRetryCount(int defaultMaxRetryCount);

    /// <summary>
    /// Sets the maximum byte size for job messages exchanged through the messaging layer.
    /// Jobs whose payload exceeds this limit will be rejected at submission time.
    /// Use -1 to disable the limit (not recommended in constrained environments).
    /// Default: <see cref="JobMasterDefaults.MaxMessageByteSize"/> (128 KB).
    /// </summary>
    /// <param name="maxMessageByteSize">The maximum message size in bytes.</param>
    public IClusterConfigSelector ClusterMaxMessageByteSize(int maxMessageByteSize);

    /// <summary>
    /// Sets the IANA time zone ID used when evaluating recurring job schedules (e.g. cron expressions).
    /// Example values: "America/New_York", "Europe/London", "UTC".
    /// Default: the local system time zone.
    /// </summary>
    /// <param name="ianaTimeZoneId">A valid IANA time zone identifier.</param>
    public IClusterConfigSelector ClusterIanaTimeZoneId(string ianaTimeZoneId);

    /// <summary>
    /// Registers a worker that picks up and executes jobs for this cluster.
    /// Optionally binds the worker to a named agent connection and configures how many jobs it pulls per cycle.
    /// </summary>
    /// <param name="workerName">Optional logical name for this worker instance.</param>
    /// <param name="agentConnectionName">The name of the agent connection this worker belongs to. Required when more than one agent connection is registered.</param>
    /// <param name="transferBatchSize">Number of jobs pulled from the master per transfer cycle. Default: 250.</param>
    public IAgentWorkerSelector AddWorker(string? workerName = null, string? agentConnectionName = null, int transferBatchSize = 250);

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
    public IClusterConfigSelector ClusterMode(ClusterMode mode);

    /// <summary>
    /// Switches this cluster to standalone mode, where the master and worker run in the same process.
    /// In standalone mode no separate agent connection is needed.
    /// Returns a selector to apply standalone-specific settings.
    /// </summary>
    public IClusterStandaloneConfigSelector UseStandaloneCluster();

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
