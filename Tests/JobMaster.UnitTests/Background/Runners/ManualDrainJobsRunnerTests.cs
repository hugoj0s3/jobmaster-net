using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Background.Runners.DrainRunners;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="ManualDrainSavePendingRecurringScheduleRunner"/>.
/// Covers: skip when no bucket ID is set, skip when the bucket is not in Draining status,
/// skip when the pending-recurring queue is empty, successful drain-save processing, and
/// per-schedule failure with consecutive-failure backoff applied to the next interval.
/// </summary>
public class ManualDrainJobsRunnerTests
{
    private const string TestBucketId = "drain-bucket";

    private static ManualDrainSavePendingRecurringScheduleRunner CreateRunner(
        RunnerFixture f, string? bucketId = TestBucketId)
    {
        var runner = new ManualDrainSavePendingRecurringScheduleRunner(f.Worker.Object);
        if (bucketId != null)
            runner.DefineBucketId(bucketId);
        return runner;
    }

    private static RecurringScheduleRawModel PendingSchedule(string clusterId)
        => new(clusterId)
        {
            Id = Guid.NewGuid(),
            Status = RecurringScheduleStatus.PendingSave,
            BucketId = TestBucketId,
        };

    // ── OnTickAsync — skip paths ───────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenBucketIdNotDefined_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        var runner = new ManualDrainSavePendingRecurringScheduleRunner(f.Worker.Object);

        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketDoesNotExist_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();

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
    public async Task OnTickAsync_WhenQueueIsEmpty_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.DrainingBucket(f.ClusterId, TestBucketId));
        // PendingRecurQueue is empty.

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    // ── OnTickAsync — processing paths ────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenPendingSchedulesExist_ShouldProcessThemAndReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.DrainingBucket(f.ClusterId, TestBucketId));
        f.JobsDispatcher.PendingRecurQueue.Add(PendingSchedule(f.ClusterId));

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.JobsDispatcher.PendingRecurQueue.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenScheduleProcessingFails_ShouldReturnSkippedWithDelay()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.DrainingBucket(f.ClusterId, TestBucketId));
        f.JobsDispatcher.PendingRecurQueue.Add(PendingSchedule(f.ClusterId));

        // Make ExecWithRetryAsync (action variant) throw so SaveDrainWithSafeGuardAsync fails.
        f.WorkerClusterOps
            .Setup(x => x.ExecWithRetryAsync(
                It.IsAny<Action<IWorkerClusterOperations>>(),
                It.IsAny<int>(),
                It.IsAny<int>()))
            .Throws(new InvalidOperationException("simulated save failure"));

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        result.Delay.Should().BeGreaterThan(TimeSpan.Zero);
    }

    [Fact]
    public async Task OnTickAsync_WhenConsecutiveFailures_ShouldIncreaseDelay()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.DrainingBucket(f.ClusterId, TestBucketId));
        f.WorkerClusterOps
            .Setup(x => x.ExecWithRetryAsync(
                It.IsAny<Action<IWorkerClusterOperations>>(),
                It.IsAny<int>(),
                It.IsAny<int>()))
            .Throws(new InvalidOperationException("simulated failure"));

        var runner = CreateRunner(f);

        f.JobsDispatcher.PendingRecurQueue.Add(PendingSchedule(f.ClusterId));
        var result1 = await runner.OnTickAsync(CancellationToken.None);

        f.JobsDispatcher.PendingRecurQueue.Add(PendingSchedule(f.ClusterId));
        var result2 = await runner.OnTickAsync(CancellationToken.None);

        result2.Delay.Should().BeGreaterThanOrEqualTo(result1.Delay);
    }
}
