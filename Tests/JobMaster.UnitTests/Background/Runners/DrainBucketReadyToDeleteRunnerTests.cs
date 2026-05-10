using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Background.Runners.DrainRunners;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="DrainBucketReadyToDeleteRunner"/>.
/// Covers: skip when no bucket ID is set, skip when the bucket is not in Draining status,
/// waiting while jobs remain, waiting while the idle period has not elapsed, and
/// transitioning to ReadyToDelete once the bucket has been job-free long enough.
/// </summary>
public class DrainBucketReadyToDeleteRunnerTests
{
    private const string TestBucketId = "drain-bucket";

    private static DrainBucketReadyToDeleteRunner CreateRunner(RunnerFixture f, string? bucketId = TestBucketId)
    {
        var runner = new DrainBucketReadyToDeleteRunner(f.Worker.Object);
        if (bucketId != null)
            runner.DefineBucketId(bucketId);
        return runner;
    }

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenBucketIdNotDefined_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        var runner = new DrainBucketReadyToDeleteRunner(f.Worker.Object); // no DefineBucketId

        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketDoesNotExist_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        // No bucket added.

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketIsNotDraining_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketIsDrainingAndHasJobs_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.DrainingBucket(f.ClusterId, TestBucketId));
        // Dispatcher reports jobs are still present.
        f.JobsDispatcher.HasJobsResult = true;

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.UpdatedBuckets.Should().BeEmpty(); // not yet transitioned
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketHasNoJobsButIdlePeriodNotElapsed_ShouldNotTransition()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.DrainingBucket(f.ClusterId, TestBucketId));
        // No jobs — but the idle timer starts now, period hasn't elapsed.
        f.JobsDispatcher.HasJobsResult = false;

        var runner = CreateRunner(f);

        // First tick: starts the idle clock.
        var result1 = await runner.OnTickAsync(CancellationToken.None);
        // Second tick immediately (idle period = 10 min, so not elapsed).
        var result2 = await runner.OnTickAsync(CancellationToken.None);

        result1.Status.Should().Be(TicketResultStatus.Success);
        result2.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.UpdatedBuckets.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenIdlePeriodHasElapsed_ShouldTransitionToReadyToDelete()
    {
        var f = RunnerFixture.Create();
        var bucket = RunnerFixture.DrainingBucket(f.ClusterId, TestBucketId);
        f.Buckets.Buckets.Add(bucket);
        f.JobsDispatcher.HasJobsResult = false;

        var runner = CreateRunner(f);

        // Tick once to start the idle clock.
        await runner.OnTickAsync(CancellationToken.None);

        // Use reflection to backdate the noJobsSinceUtc field so the idle period appears elapsed.
        var field = typeof(DrainBucketReadyToDeleteRunner)
            .GetField("noJobsSinceUtc", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        field.SetValue(runner, DateTime.UtcNow.AddMinutes(-15));

        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.Buckets.UpdatedBuckets.Should().ContainSingle(b => b.Id == TestBucketId);
        f.Buckets.UpdatedBuckets[0].Status.Should().Be(BucketStatus.ReadyToDelete);
    }

    [Fact]
    public async Task OnTickAsync_WhenJobsArriveAfterIdleStarted_ShouldResetIdleTimer()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.DrainingBucket(f.ClusterId, TestBucketId));
        f.JobsDispatcher.HasJobsResult = false;

        var runner = CreateRunner(f);

        // Start idle clock.
        await runner.OnTickAsync(CancellationToken.None);

        // Jobs reappear — idle clock should reset to null.
        f.JobsDispatcher.HasJobsResult = true;
        await runner.OnTickAsync(CancellationToken.None);

        // Check that noJobsSinceUtc was cleared.
        var field = typeof(DrainBucketReadyToDeleteRunner)
            .GetField("noJobsSinceUtc", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        field.GetValue(runner).Should().BeNull();
    }
}
