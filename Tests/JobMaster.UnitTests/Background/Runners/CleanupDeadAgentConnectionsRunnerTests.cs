using FluentAssertions;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Background.Runners;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="CleanupDeadAgentConnectionsRunner"/>.
/// Covers: no-op when all connections are alive, deletion of connections past the 10-minute
/// dead threshold, preservation of connections within the threshold, and the protected-connection
/// path where deletion is skipped even when the connection is dead.
/// </summary>
public class CleanupDeadAgentConnectionsRunnerTests
{
    // Connections older than 10 minutes are eligible for deletion.
    private const int DeadThresholdMinutes = 10;

    private static AgentConnectionModel FreshConnection(string clusterId, string name)
        => new(clusterId)
        {
            Id = new AgentConnectionId(clusterId, name),
            LastHeartbeatAt = DateTime.UtcNow,
            FingerprintCreatedAt = DateTime.UtcNow,
        };

    private static AgentConnectionModel StaleConnection(string clusterId, string name, bool protect = false)
        => new(clusterId)
        {
            Id = new AgentConnectionId(clusterId, name),
            LastHeartbeatAt = DateTime.UtcNow.AddMinutes(-(DeadThresholdMinutes + 1)),
            FingerprintCreatedAt = DateTime.UtcNow.AddHours(-2),
            ProtectConnectionChanges = protect,
        };

    // â”€â”€ OnTickAsync â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    [Fact]
    public async Task OnTickAsync_WhenAllConnectionsAlive_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.AgentConnectionService.Connections.Add(FreshConnection(f.ClusterId, "conn-1"));
        f.AgentConnectionService.Connections.Add(FreshConnection(f.ClusterId, "conn-2"));

        var runner = new CleanupDeadAgentConnectionsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.AgentConnectionService.DeletedConnectionIds.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenNoConnections_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();

        var runner = new CleanupDeadAgentConnectionsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.AgentConnectionService.DeletedConnectionIds.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenConnectionIsPastDeadThreshold_ShouldDeleteIt()
    {
        var f = RunnerFixture.Create();
        f.AgentConnectionService.Connections.Add(StaleConnection(f.ClusterId, "stale-conn"));
        f.AgentConnectionService.Connections.Add(FreshConnection(f.ClusterId, "alive-conn"));

        var runner = new CleanupDeadAgentConnectionsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.AgentConnectionService.DeletedConnectionIds.Should().ContainSingle();
        f.AgentConnectionService.Connections.Should().ContainSingle(c => c.Id.Name == "alive-conn");
    }

    [Fact]
    public async Task OnTickAsync_WhenConnectionIsProtected_ShouldNotDeleteIt()
    {
        var f = RunnerFixture.Create();
        // Protected connection is stale but must not be removed.
        f.AgentConnectionService.Connections.Add(StaleConnection(f.ClusterId, "protected-conn", protect: true));

        var runner = new CleanupDeadAgentConnectionsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.AgentConnectionService.DeletedConnectionIds.Should().BeEmpty();
        f.AgentConnectionService.Connections.Should().ContainSingle(c => c.Id.Name == "protected-conn");
    }

    [Fact]
    public async Task OnTickAsync_WhenConnectionIsRecentlyStale_ShouldNotDeleteIt()
    {
        var f = RunnerFixture.Create();
        // Heartbeat is just 2 minutes ago â€” within the 10-minute threshold.
        var recent = new AgentConnectionModel(f.ClusterId)
        {
            Id = new AgentConnectionId(f.ClusterId, "recent-conn"),
            LastHeartbeatAt = DateTime.UtcNow.AddMinutes(-2),
            FingerprintCreatedAt = DateTime.UtcNow.AddHours(-1),
        };
        f.AgentConnectionService.Connections.Add(recent);

        var runner = new CleanupDeadAgentConnectionsRunner(f.Worker.Object);
        await runner.OnTickAsync(CancellationToken.None);

        f.AgentConnectionService.DeletedConnectionIds.Should().BeEmpty();
    }

    [Theory]
    [InlineData(JobMasterConstants.StandaloneAgentConnName)]
    [InlineData(JobMasterConstants.MasterFallbackAgentConnName)]
    public async Task OnTickAsync_WhenReservedConnectionIsStale_ShouldNeverDeleteIt(string reservedName)
    {
        var f = RunnerFixture.Create();
        // Reserved connections are expected to sit dead for long stretches (standalone with no
        // standalone worker running, fallback with no fallback bucket active) — must never be deleted.
        f.AgentConnectionService.Connections.Add(StaleConnection(f.ClusterId, reservedName));

        var runner = new CleanupDeadAgentConnectionsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.AgentConnectionService.DeletedConnectionIds.Should().BeEmpty();
        f.AgentConnectionService.Connections.Should().ContainSingle(c => c.Id.Name == reservedName);
    }
}
