using FluentAssertions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Background.Runners;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="DeadWorkerCleanupRunner"/>.
/// Covers: no-op when all workers are alive, skipping the current worker even if dead,
/// preserving workers within their grace period or with a recent heartbeat, and deleting
/// workers that are genuinely eligible for cleanup.
/// </summary>
public class DeadWorkerCleanupRunnerTests
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private const string TestClusterId = "test-cluster";

    private static AgentWorkerModel DeadWorker(
        string id,
        DateTime? lastHeartbeat = null,
        DateTime? stopRequestedAt = null,
        TimeSpan? stopGracePeriod = null)
        => new(TestClusterId)
        {
            Id = id,
            IsAlive = false,
            LastHeartbeat = lastHeartbeat ?? DateTime.UtcNow.AddHours(-2),
            StopRequestedAt = stopRequestedAt,
            StopGracePeriod = stopGracePeriod,
        };

    private static AgentWorkerModel AliveWorker(string id)
        => new(TestClusterId) { Id = id, IsAlive = true, LastHeartbeat = DateTime.UtcNow };

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenNoDeadWorkers_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.AgentWorkers.Workers.Add(AliveWorker("alive-1"));
        f.AgentWorkers.Workers.Add(AliveWorker("alive-2"));

        var runner = new DeadWorkerCleanupRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.AgentWorkers.Workers.Should().HaveCount(2);
    }

    [Fact]
    public async Task OnTickAsync_WhenDeadWorkerIsCurrentWorker_ShouldNotDeleteIt()
    {
        var f = RunnerFixture.Create();

        // Add the current worker as dead — the runner must skip it.
        var self = DeadWorker(f.WorkerId);
        f.AgentWorkers.Workers.Add(self);

        var runner = new DeadWorkerCleanupRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.AgentWorkers.Workers.Should().ContainSingle(w => w.Id == f.WorkerId);
    }

    [Fact]
    public async Task OnTickAsync_WhenDeadWorkerIsWithinGracePeriod_ShouldNotDeleteIt()
    {
        var f = RunnerFixture.Create();

        // StopGracePeriod of 1 hour means the worker is still within its grace window.
        var worker = DeadWorker(
            "dead-grace",
            lastHeartbeat: DateTime.UtcNow.AddMinutes(-1),
            stopGracePeriod: TimeSpan.FromHours(1));

        f.AgentWorkers.Workers.Add(worker);

        var runner = new DeadWorkerCleanupRunner(f.Worker.Object);
        await runner.OnTickAsync(CancellationToken.None);

        f.AgentWorkers.Workers.Should().ContainSingle(w => w.Id == "dead-grace");
    }

    [Fact]
    public async Task OnTickAsync_WhenDeadWorkerHasRecentHeartbeat_ShouldNotDeleteIt()
    {
        var f = RunnerFixture.Create();

        // Very recent heartbeat — within the maxTimeToLive window.
        var worker = DeadWorker("dead-recent", lastHeartbeat: DateTime.UtcNow.AddSeconds(-30));
        f.AgentWorkers.Workers.Add(worker);

        var runner = new DeadWorkerCleanupRunner(f.Worker.Object);
        await runner.OnTickAsync(CancellationToken.None);

        f.AgentWorkers.Workers.Should().ContainSingle(w => w.Id == "dead-recent");
    }

    [Fact]
    public async Task OnTickAsync_WhenDeadWorkerIsEligibleForCleanup_ShouldDeleteIt()
    {
        var f = RunnerFixture.Create();

        // Old heartbeat, no grace period — clearly eligible for cleanup.
        var worker = DeadWorker("dead-old", lastHeartbeat: DateTime.UtcNow.AddHours(-2));
        f.AgentWorkers.Workers.Add(worker);
        f.AgentWorkers.Workers.Add(AliveWorker("alive-1"));

        var runner = new DeadWorkerCleanupRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.AgentWorkers.Workers.Should().NotContain(w => w.Id == "dead-old");
        f.AgentWorkers.Workers.Should().ContainSingle(w => w.Id == "alive-1");
    }
}
