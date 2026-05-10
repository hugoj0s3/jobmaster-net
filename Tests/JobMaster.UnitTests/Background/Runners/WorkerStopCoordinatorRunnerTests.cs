using FluentAssertions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Background.Runners;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="WorkerStopCoordinatorRunner"/>.
/// Covers: normal tick with no stop signals, triggering an immediate stop when the
/// immediate-stop lock is detected, triggering a graceful stop when the graceful-stop lock
/// is detected, returning Failed once a stop-immediately flag is already set, and waiting
/// during the grace period before escalating to an immediate stop.
/// </summary>
public class WorkerStopCoordinatorRunnerTests
{
    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenNoStopSignals_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        // Default fixture: no locks held, StopRequested = false, StopImmediatelyRequested = false.

        var runner = new WorkerStopCoordinatorRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Worker.Verify(x => x.StopImmediatelyAsync(), Times.Never);
    }

    [Fact]
    public async Task OnTickAsync_WhenImmediateStopLockIsHeld_ShouldCallStopImmediately()
    {
        var f = RunnerFixture.Create();
        var lockKeys = new JobMasterLockKeys(f.ClusterId);

        // Pre-lock the immediate-stop key for this worker.
        f.Locker.ForceAddLock(lockKeys.WorkerImmediateStopLock(f.WorkerId), TimeSpan.FromMinutes(5));

        f.Worker.Setup(x => x.StopImmediatelyAsync()).Returns(Task.CompletedTask);

        var runner = new WorkerStopCoordinatorRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Worker.Verify(x => x.StopImmediatelyAsync(), Times.Once);
    }

    [Fact]
    public async Task OnTickAsync_WhenGracefulStopLockIsHeld_ShouldRequestStop()
    {
        var f = RunnerFixture.Create();
        var lockKeys = new JobMasterLockKeys(f.ClusterId);

        // Pre-lock the graceful-stop key for this worker.
        f.Locker.ForceAddLock(lockKeys.WorkerGracefulStopLock(f.WorkerId), TimeSpan.FromMinutes(5));

        var runner = new WorkerStopCoordinatorRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Worker.Verify(x => x.RequestStop(), Times.Once);
        f.Worker.Verify(x => x.StopImmediatelyAsync(), Times.Never);
    }

    [Fact]
    public async Task OnTickAsync_WhenStopImmediatelyAlreadySet_ShouldReturnFailed()
    {
        var f = RunnerFixture.Create();
        // Stop has already been requested immediately — runner should return Failed.
        f.Worker.SetupGet(x => x.StopImmediatelyRequested).Returns(true);

        var runner = new WorkerStopCoordinatorRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Failed);
    }

    [Fact]
    public async Task OnTickAsync_WhenGracePeriodHasNotElapsed_ShouldNotEscalateToImmediate()
    {
        var f = RunnerFixture.Create();
        // StopRequested = true, grace period of 10 minutes starting just now.
        f.Worker.SetupGet(x => x.StopRequested).Returns(true);
        f.Worker.SetupGet(x => x.StopRequestedAt).Returns(DateTime.UtcNow.AddSeconds(-5));
        f.Worker.SetupGet(x => x.StopGracePeriod).Returns(TimeSpan.FromMinutes(10));

        var runner = new WorkerStopCoordinatorRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Worker.Verify(x => x.StopImmediatelyAsync(), Times.Never);
    }

    [Fact]
    public async Task OnTickAsync_WhenGracePeriodHasElapsed_ShouldEscalateToImmediateStop()
    {
        var f = RunnerFixture.Create();
        // StopRequested = true, grace period expired (requested 5 minutes ago, period = 1 min).
        f.Worker.SetupGet(x => x.StopRequested).Returns(true);
        f.Worker.SetupGet(x => x.StopRequestedAt).Returns(DateTime.UtcNow.AddMinutes(-5));
        f.Worker.SetupGet(x => x.StopGracePeriod).Returns(TimeSpan.FromMinutes(1));
        f.Worker.Setup(x => x.StopImmediatelyAsync()).Returns(Task.CompletedTask);

        var runner = new WorkerStopCoordinatorRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Worker.Verify(x => x.StopImmediatelyAsync(), Times.Once);
    }
}
