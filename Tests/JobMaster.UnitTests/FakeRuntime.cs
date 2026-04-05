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

    public OperationLimiter GetOperationLimiterForCluster(string clusterId)
    {
        return new OperationLimiter(null);
    }

    public OperationLimiter GetOperationLimiterForAgent(string clusterId, string agentConnectionIdOrName)
    {
        return new OperationLimiter(int.MaxValue);
    }

    public int CountWorkersForCluster(string clusterId) => 1;

    

    public void Stop() { }
}