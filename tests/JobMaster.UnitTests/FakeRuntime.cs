using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Background.Cancellation;

namespace JobMaster.UnitTests;

internal sealed class FakeRuntime : IJobMasterRuntime
{
    public bool Started { get; }
    public DateTime? StartedAt { get; } = DateTime.UtcNow;
    public DateTime? StartingAt { get; } = DateTime.UtcNow;
    public StopControl StopControl { get; }
    public JobMasterStopControlFactory StopControlFactory { get; }

    public FakeRuntime() : this(started: true) { }

    public FakeRuntime(bool started)
    {
        Started = started;
        StopControlFactory = new JobMasterStopControlFactory();
        StopControl = StopControlFactory.ForRoot();
    }

    public IReadOnlyList<IJobMasterBackgroundAgentWorker> GetAllWorkers() => new List<IJobMasterBackgroundAgentWorker>();

    public bool IsOnWarmUpTime() => Started;

    public Task StartAsync(IServiceProvider serviceProvider) => throw new NotSupportedException();
}
