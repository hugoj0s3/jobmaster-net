namespace JobMaster.Abstractions.Models;

/// <summary>
/// Represents the operational status of an agent worker within the cluster.
/// </summary>
public enum AgentWorkerStatus
{
    /// <summary>The worker is running and processing jobs.</summary>
    Active,
    /// <summary>The worker has received a stop request and is finishing in-flight work.</summary>
    Stopping,
    /// <summary>The worker is no longer responding and is considered lost.</summary>
    Dead
}
