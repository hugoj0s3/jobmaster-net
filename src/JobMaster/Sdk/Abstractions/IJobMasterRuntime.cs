using JobMaster.Sdk.Abstractions.Background;

namespace JobMaster.Sdk.Abstractions;

internal interface IJobMasterRuntime
{
    bool Started { get; }
    DateTime? StartedAt { get; }
    DateTime? StartingAt { get; }
    
    IReadOnlyList<IJobMasterBackgroundAgentWorker> GetAllWorkers();

    /// <summary>
    /// Determines if the runtime started recently (within the last 2.5 minutes).
    /// </summary>
    /// <returns>True if the runtime started within the last 2.5 minutes, false otherwise.</returns>
    bool IsOnWarmUpTime();

    Task StartAsync(IServiceProvider serviceProvider);

    int CountWorkersForCluster(string clusterId);
    
    void Stop();
}