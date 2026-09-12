using FluentAssertions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Background.Runners.CleanUpData;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="DeleteExpiredGenericRecordsRunner"/>.
/// Covers: skip when stop is requested, lock contention path, successful delete with no
/// burst (zero deleted), burst-limiter path when a full batch is deleted, and verifying the
/// repository is called exactly once per tick.
/// </summary>
public class DeleteExpiredGenericRecordsRunnerTests
{
    [Fact]
    public async Task OnTickAsync_WhenStopRequested_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.Worker.SetupGet(x => x.StopRequested).Returns(true);

        var runner = new DeleteExpiredGenericRecordsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        f.GenericRecords.DeleteExpiredCallCount.Should().Be(0);
    }

    [Fact]
    public async Task OnTickAsync_WhenLockIsAlreadyHeld_ShouldReturnLocked()
    {
        var f = RunnerFixture.Create();
        f.Locker.BlockAllLocks = true;

        var runner = new DeleteExpiredGenericRecordsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
        f.GenericRecords.DeleteExpiredCallCount.Should().Be(0);
    }

    [Fact]
    public async Task OnTickAsync_WhenNoExpiredRecords_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.GenericRecords.DeleteExpiredReturnValue = 0;

        var runner = new DeleteExpiredGenericRecordsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.GenericRecords.DeleteExpiredCallCount.Should().Be(1);
    }

    [Fact]
    public async Task OnTickAsync_WhenFullBatchDeleted_ShouldReturnSuccessWithShorterDelay()
    {
        var f = RunnerFixture.Create();
        // Return the full batch size (50) so the burst limiter shortens the next interval.
        f.GenericRecords.DeleteExpiredReturnValue = 50;

        var runner = new DeleteExpiredGenericRecordsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        // Burst interval (5 min) is much shorter than SucceedInterval (1 h).
        result.Delay.Should().BeLessThan(runner.SucceedInterval);
    }

    [Fact]
    public async Task OnTickAsync_WhenPartialBatchDeleted_ShouldReturnSuccessWithNormalDelay()
    {
        var f = RunnerFixture.Create();
        // Return fewer than the batch size — burst limiter stays dormant.
        f.GenericRecords.DeleteExpiredReturnValue = 5;

        var runner = new DeleteExpiredGenericRecordsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        result.Delay.Should().Be(runner.SucceedInterval);
    }

    [Fact]
    public async Task OnTickAsync_ReleasesLockAfterSuccess()
    {
        var f = RunnerFixture.Create();
        f.GenericRecords.DeleteExpiredReturnValue = 0;

        var runner = new DeleteExpiredGenericRecordsRunner(f.Worker.Object);
        await runner.OnTickAsync(CancellationToken.None);

        // After the tick the lock should be released, allowing a second tick to acquire it.
        var result2 = await runner.OnTickAsync(CancellationToken.None);
        result2.Status.Should().Be(TicketResultStatus.Success);
        f.GenericRecords.DeleteExpiredCallCount.Should().Be(2);
    }
}
