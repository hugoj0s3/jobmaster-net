using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Background.Cancellation;

namespace JobMaster.UnitTests;

internal sealed class FakeRuntime : IJobMasterRuntime
{
    public bool Started { get; }
    public DateTime? StartedAt { get; } = DateTime.UtcNow;
    public DateTime? StartingAt { get; } = DateTime.UtcNow;
    public StopScope StopScope { get; }
    public JobMasterStopScopeFactory StopScopeFactory { get; }

    public FakeRuntime() : this(started: true) { }

    public FakeRuntime(bool started)
    {
        Started = started;
        StopScopeFactory = new JobMasterStopScopeFactory();
        StopScope = StopScopeFactory.ForRoot();
    }

    public IReadOnlyList<IJobMasterBackgroundAgentWorker> GetAllWorkers() => new List<IJobMasterBackgroundAgentWorker>();

    public bool IsOnWarmUpTime() => Started;

    public Task StartAsync(IServiceProvider serviceProvider) => throw new NotSupportedException();
}
