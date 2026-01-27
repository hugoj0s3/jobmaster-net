using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;

namespace JobMaster.UnitTests;

internal sealed class FakeRuntime : IJobMasterRuntime
{
    public bool Started { get; }
    public DateTime? StartedAt { get; } = DateTime.UtcNow;
    public DateTime? StartingAt { get; } = DateTime.UtcNow;

    public FakeRuntime(bool started)
    {
        Started = started;
    }

    public IReadOnlyList<IJobMasterBackgroundAgentWorker> GetAllWorkers() => new List<IJobMasterBackgroundAgentWorker>();

    public bool IsOnWarmUpTime() => Started;

    public Task StartAsync(IServiceProvider serviceProvider) => throw new NotSupportedException();
    public Task StopImmediatelyAsync() => Task.CompletedTask;

    public OperationThrottler GetOperationThrottlerForCluster(string clusterId)
    {
        return new OperationThrottler(null);
    }

    public OperationThrottler GetOperationThrottlerForAgent(string clusterId, string agentConnectionIdOrName)
    {
        return new OperationThrottler(int.MaxValue);
    }

    public int CountWorkersForCluster(string clusterId) => 1;

    

    public void Stop() { }
}