using FluentAssertions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Background.Runners;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="CleanupDeadHostsRunner"/>.
/// Covers: no-op when all hosts are alive, deletion of hosts past the 5-minute dead
/// threshold, preservation of hosts with a recent heartbeat, and bulk deletion when
/// multiple dead hosts are present simultaneously.
/// </summary>
public class CleanupDeadHostsRunnerTests
{
    // Hosts with no heartbeat in the last 5 minutes are eligible for deletion.
    private const int DeadThresholdMinutes = 5;

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenNoHosts_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();

        var runner = new CleanupDeadHostsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.HostService.DeletedHostIds.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenAllHostsAlive_ShouldNotDeleteAny()
    {
        var f = RunnerFixture.Create();
        f.HostService.Hosts.Add(RunnerFixture.AliveHost(f.ClusterId, "host-1"));
        f.HostService.Hosts.Add(RunnerFixture.AliveHost(f.ClusterId, "host-2"));

        var runner = new CleanupDeadHostsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.HostService.DeletedHostIds.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenHostIsPastDeadThreshold_ShouldDeleteIt()
    {
        var f = RunnerFixture.Create();
        f.HostService.Hosts.Add(RunnerFixture.DeadHost(f.ClusterId, "dead-host"));
        f.HostService.Hosts.Add(RunnerFixture.AliveHost(f.ClusterId, "alive-host"));

        var runner = new CleanupDeadHostsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.HostService.DeletedHostIds.Should().HaveCount(1);
        f.HostService.Hosts.Should().ContainSingle(h => h.Id.HostDisplayName == "alive-host");
    }

    [Fact]
    public async Task OnTickAsync_WhenHostHasRecentHeartbeat_ShouldNotDeleteIt()
    {
        var f = RunnerFixture.Create();
        // Heartbeat is 2 minutes ago — within the 5-minute threshold.
        var recent = new HostModel(f.ClusterId)
        {
            Id = new HostId(f.ClusterId, "recent-host"),
            LastHeartbeat = DateTime.UtcNow.AddMinutes(-2),
        };
        f.HostService.Hosts.Add(recent);

        var runner = new CleanupDeadHostsRunner(f.Worker.Object);
        await runner.OnTickAsync(CancellationToken.None);

        f.HostService.DeletedHostIds.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenMultipleDeadHosts_ShouldDeleteAllOfThem()
    {
        var f = RunnerFixture.Create();
        f.HostService.Hosts.Add(RunnerFixture.DeadHost(f.ClusterId, "dead-1"));
        f.HostService.Hosts.Add(RunnerFixture.DeadHost(f.ClusterId, "dead-2"));
        f.HostService.Hosts.Add(RunnerFixture.AliveHost(f.ClusterId, "alive-1"));

        var runner = new CleanupDeadHostsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.HostService.DeletedHostIds.Should().HaveCount(2);
        f.HostService.Hosts.Should().ContainSingle(h => h.Id.HostDisplayName == "alive-1");
    }
}
