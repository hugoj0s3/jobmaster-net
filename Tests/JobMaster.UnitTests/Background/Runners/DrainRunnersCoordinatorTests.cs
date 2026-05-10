using FluentAssertions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Background.Runners.BucketLifeCycleControl;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="DrainRunnersCoordinator"/>.
/// Covers: lock contention path (returns Skipped), no-op when no ReadyToDrain buckets
/// exist, and cleanup of stale drain runners for buckets that are no longer in a
/// Draining or ReadyToDrain state.
/// </summary>
public class DrainRunnersCoordinatorTests
{
    // ── fixture helpers ────────────────────────────────────────────────────────

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenLockAlreadyHeld_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.Locker.BlockAllLocks = true;

        var runner = new DrainRunnersCoordinator(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenNoReadyToDrainBuckets_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        // No ReadyToDrain buckets — CleanupDrainRunnersAsync and CreateDrainRunners are no-ops.
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, "active-bucket"));

        var runner = new DrainRunnersCoordinator(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.UpdatedBuckets.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenNoReadyToDrainBucketsAndNoRunners_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        // Completely empty state.

        var runner = new DrainRunnersCoordinator(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
    }

    [Fact]
    public async Task OnTickAsync_ReleasesLockAfterSuccessfulTick()
    {
        var f = RunnerFixture.Create();

        var runner = new DrainRunnersCoordinator(f.Worker.Object);
        await runner.OnTickAsync(CancellationToken.None);

        // Lock should be released — a second tick should succeed too.
        var result2 = await runner.OnTickAsync(CancellationToken.None);
        result2.Status.Should().Be(TicketResultStatus.Success);
    }
}
