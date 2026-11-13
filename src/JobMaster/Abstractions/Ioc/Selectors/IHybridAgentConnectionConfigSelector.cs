namespace JobMaster.Abstractions.Ioc.Selectors;

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
