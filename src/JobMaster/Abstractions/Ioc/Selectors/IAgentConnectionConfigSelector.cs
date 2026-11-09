using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;

namespace JobMaster.Abstractions.Ioc.Selectors;

/// <summary>
/// Provider-selection contract for an agent connection target (either a standalone agent connection
/// or the transport/execution leg of a hybrid agent connection).
/// </summary>
public interface IAgentConnectionProviderSelector
{
    internal IAgentConnectionProviderSelector AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig);

    internal IAgentConnectionProviderSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);

    internal IAgentConnectionProviderSelector AgentRepoType(string repoType);

    internal IAgentConnectionProviderSelector AgentConnString(string connString);
}

/// <summary>
/// Fluent configuration for a hybrid agent connection that uses separate storage providers
/// for transport (saving pending jobs and recurring schedules) and execution (dispatching and running jobs).
/// </summary>
public interface IHybridAgentConnectionConfigSelector
{
    /// <summary>
    /// Sets the logical name of this agent connection.
    /// </summary>
    /// <param name="agentConnName">A unique name for this agent connection within the cluster.</param>
    public IHybridAgentConnectionConfigSelector AgentConnName(string agentConnName);

    /// <summary>
    /// When enabled, the agent registers a combined fingerprint of both transport and execution connections
    /// on the master at startup and validates it on subsequent restarts.
    /// </summary>
    /// <param name="protectConnectionChanges"><c>true</c> to enable fingerprint protection; <c>false</c> to allow unrestricted changes.</param>
    public IHybridAgentConnectionConfigSelector ProtectConnectionChanges(bool protectConnectionChanges);

    /// <summary>
    /// Configures the transport provider used for saving pending jobs and saving pending recurring schedules.
    /// </summary>
    public IHybridAgentConnectionConfigSelector Transport(Action<IAgentConnectionProviderSelector> configure);

    /// <summary>
    /// Configures the execution provider used for dispatching jobs to processing buckets and executing them.
    /// </summary>
    public IHybridAgentConnectionConfigSelector Execution(Action<IAgentConnectionProviderSelector> configure);
}

/// <summary>
/// Fluent configuration for an agent connection registered on a cluster.
/// An agent connection represents the communication channel between the master scheduler
/// and a remote worker node (agent). Each connection has its own identity, data store,
/// and optional protection settings.
/// All methods return the same selector instance to allow method chaining.
/// </summary>
public interface IAgentConnectionConfigSelector : IAgentConnectionProviderSelector
{
    /// <summary>
    /// Sets the logical name of this agent connection.
    /// This name is used to associate workers with the connection and to identify it in logs and metrics.
    /// Allowed characters: letters (A–Z, a–z), digits (0–9), hyphens (-), underscores (_). Max 50 characters.
    /// </summary>
    /// <param name="agentConnName">A unique name for this agent connection within the cluster.</param>
    public IAgentConnectionConfigSelector AgentConnName(string agentConnName);

    /// <summary>
    /// When enabled, the agent registers a fingerprint of this connection on the master at startup.
    /// On subsequent restarts the master validates the fingerprint, preventing the core endpoint
    /// (e.g. server host and database name) from being silently swapped.
    /// Credentials and other configuration values can still be updated freely.
    /// </summary>
    /// <param name="protectConnectionChanges"><c>true</c> to enable fingerprint protection; <c>false</c> to allow unrestricted changes.</param>
    public IAgentConnectionConfigSelector ProtectConnectionChanges(bool protectConnectionChanges);

    /// <summary>
    /// Switches this agent connection into hybrid mode, allowing separate providers to be configured
    /// for <see cref="IHybridAgentConnectionConfigSelector.Transport"/> and <see cref="IHybridAgentConnectionConfigSelector.Execution"/>.
    /// </summary>
    public IHybridAgentConnectionConfigSelector UseHybrid();
}
