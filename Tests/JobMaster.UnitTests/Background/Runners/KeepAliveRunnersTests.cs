using FluentAssertions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Runners;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="KeepAliveWorkerRunner"/>, <see cref="KeepAliveAgentConnectionRunner"/>,
/// and <see cref="KeepAliveHostRunner"/>.
/// Covers: successful heartbeat emission with the correct resource type and ID, and the host
/// runner's additional <c>UpdateStatsAsync</c> call on every tick.
/// </summary>
public class KeepAliveRunnersTests
{
    // ── KeepAliveWorkerRunner ─────────────────────────────────────────────────

    [Fact]
    public async Task KeepAliveWorkerRunner_OnTickAsync_ShouldSendAgentWorkerHeartbeat()
    {
        var f = RunnerFixture.Create();

        var runner = new KeepAliveWorkerRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.HeartbeatService.HeartbeatCalls.Should().ContainSingle(
            c => c.Type == ResourceHeartbeatType.AgentWorker && c.ResourceId == f.WorkerId);
    }

    [Fact]
    public async Task KeepAliveWorkerRunner_OnTickAsync_ShouldReturnSuccessOnEveryTick()
    {
        var f = RunnerFixture.Create();

        var runner = new KeepAliveWorkerRunner(f.Worker.Object);
        await runner.OnTickAsync(CancellationToken.None);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.HeartbeatService.HeartbeatCalls.Should().HaveCount(2);
    }

    // ── KeepAliveAgentConnectionRunner ────────────────────────────────────────

    [Fact]
    public async Task KeepAliveAgentConnectionRunner_OnTickAsync_ShouldSendAgentConnectionHeartbeat()
    {
        var f = RunnerFixture.Create();
        var expectedId = f.Worker.Object.AgentConnectionId.IdValue;

        var runner = new KeepAliveAgentConnectionRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.HeartbeatService.HeartbeatCalls.Should().ContainSingle(
            c => c.Type == ResourceHeartbeatType.AgentConnection && c.ResourceId == expectedId);
    }

    // ── KeepAliveHostRunner ───────────────────────────────────────────────────

    [Fact]
    public async Task KeepAliveHostRunner_OnTickAsync_ShouldSendHostHeartbeat()
    {
        var f = RunnerFixture.Create();
        var expectedId = f.Worker.Object.HostId.IdValue;

        var runner = new KeepAliveHostRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.HeartbeatService.HeartbeatCalls.Should().ContainSingle(
            c => c.Type == ResourceHeartbeatType.Host && c.ResourceId == expectedId);
    }

    [Fact]
    public async Task KeepAliveHostRunner_OnTickAsync_ShouldCallUpdateStats()
    {
        var f = RunnerFixture.Create();

        var runner = new KeepAliveHostRunner(f.Worker.Object);
        await runner.OnTickAsync(CancellationToken.None);

        f.HostService.UpdateStatsCallCount.Should().Be(1);
    }
}
