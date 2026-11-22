using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Background.Cancellation;

namespace JobMaster.Sdk.Abstractions;

internal interface IJobMasterRuntime
{
    bool Started { get; }
    DateTime? StartedAt { get; }
    DateTime? StartingAt { get; }
    
    StopScope StopScope { get; }

    JobMasterStopScopeFactory StopScopeFactory { get; }
    
    IReadOnlyList<IJobMasterBackgroundAgentWorker> GetAllWorkers();

    /// <summary>
    /// Determines if the runtime started recently (within the last 2.5 minutes).
    /// </summary>
    /// <returns>True if the runtime started within the last 2.5 minutes, false otherwise.</returns>
    bool IsOnWarmUpTime();

    Task StartAsync(IServiceProvider serviceProvider);
}