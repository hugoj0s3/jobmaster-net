using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;

namespace JobMaster.Abstractions.Ioc.Selectors;

/// <summary>
/// Fluent configuration for an agent connection registered on a cluster.
/// An agent connection represents the communication channel between the master scheduler
/// and a remote worker node (agent). Each connection has its own identity, data store,
/// and optional protection settings.
/// All methods return the same selector instance to allow method chaining.
/// </summary>
public interface IAgentConnectionConfigSelector
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

    internal IAgentConnectionConfigSelector AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig);

    internal IAgentConnectionConfigSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);

    internal IAgentConnectionConfigSelector AgentRepoType(string repoType);
    internal IAgentConnectionConfigSelector AgentConnString(string connString);
    internal IAgentConnectionConfigSelector RuntimeDbOperationLimit(int limit);
}
