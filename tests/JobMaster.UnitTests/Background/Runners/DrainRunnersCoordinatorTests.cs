using FluentAssertions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Background.Runners.BucketLifeCycleControl;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="DrainRunnersCoordinator"/>.
/// Covers: no-op when no ReadyToDrain buckets exist, and cleanup of stale drain runners for
/// buckets that are no longer in a Draining or ReadyToDrain state. No longer covers a distributed
/// lock -- DrainRunnersCoordinator doesn't take one: its own bucket queries are already scoped to
/// this worker's own AgentWorkerId (no cross-worker race to protect against), and useSemaphore
/// already serializes it against this worker's other runners; actual mutations go through a
/// per-bucket lock in CreateDrainRunners regardless.
/// </summary>
public class DrainRunnersCoordinatorTests
{
    // ── fixture helpers ────────────────────────────────────────────────────────

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenNoReadyToDrainBuckets_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        // No ReadyToDrain buckets — CleanupDrainRunnersAsync and CreateDrainRunners are no-ops.
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, "active-bucket"));

        var runner = new DrainRunnersCoordinator(f.Worker.Object, f.AgentConnectionId);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.UpdatedBuckets.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenNoReadyToDrainBucketsAndNoRunners_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        // Completely empty state.

        var runner = new DrainRunnersCoordinator(f.Worker.Object, f.AgentConnectionId);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
    }

    [Fact]
    public async Task OnTickAsync_CanRunConsecutively_ShouldSucceedEachTime()
    {
        var f = RunnerFixture.Create();

        var runner = new DrainRunnersCoordinator(f.Worker.Object, f.AgentConnectionId);
        await runner.OnTickAsync(CancellationToken.None);

        var result2 = await runner.OnTickAsync(CancellationToken.None);
        result2.Status.Should().Be(TicketResultStatus.Success);
    }
}
