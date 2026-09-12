using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Background.Runners.SavePendingRecurringSchedule;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="PollingSaveRecurringScheduleRunner"/>.
/// Covers: skip when no bucket ID is set, skip for non-Active/Completing bucket status,
/// skip when the queue is empty, successful processing of pending recurring schedules, and
/// per-schedule failure re-queuing with consecutive-failure backoff.
/// </summary>
public class PollingSaveRecurringScheduleRunnerTests
{
    private const string TestBucketId = "test-bucket";

    private static PollingSaveRecurringScheduleRunner CreateRunner(
        RunnerFixture f, string? bucketId = TestBucketId)
    {
        var runner = new PollingSaveRecurringScheduleRunner(f.Worker.Object);
        if (bucketId != null)
            runner.DefineBucketId(bucketId);
        return runner;
    }

    private static RecurringScheduleRawModel PendingSchedule(string clusterId, string bucketId)
        => new(clusterId)
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = RecurringScheduleStatus.PendingSave,
            BucketId = bucketId,
        };

    // ── OnTickAsync — skip paths ───────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenBucketIdNotDefined_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        var runner = new PollingSaveRecurringScheduleRunner(f.Worker.Object); // no DefineBucketId

        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketDoesNotExist_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        // No bucket added — Get returns null.

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketIsNotActiveOrCompleting_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(new BucketModel(f.ClusterId)
        {
            Id = TestBucketId,
            Status = BucketStatus.ReadyToDelete,
        });

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenQueueIsEmpty_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
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
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        f.JobsDispatcher.PendingRecurQueue.Add(PendingSchedule(f.ClusterId, TestBucketId));

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.JobsDispatcher.PendingRecurQueue.Should().BeEmpty(); // pulled from queue
        f.JobsDispatcher.RequeuedRecurSchedules.Should().BeEmpty(); // no failure re-queue
    }

    [Fact]
    public async Task OnTickAsync_WhenCompletingBucketHasPendingSchedules_ShouldProcessThem()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.CompletingBucket(f.ClusterId, TestBucketId));
        f.JobsDispatcher.PendingRecurQueue.Add(PendingSchedule(f.ClusterId, TestBucketId));

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
    }

    [Fact]
    public async Task OnTickAsync_WhenScheduleProcessingFails_ShouldRequeueScheduleAndReturnSkippedWithDelay()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        f.JobsDispatcher.PendingRecurQueue.Add(PendingSchedule(f.ClusterId, TestBucketId));

        // Make ExecWithRetryAsync throw so SaveRecurringScheduleAsync fails.
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
        f.JobsDispatcher.RequeuedRecurSchedules.Should().HaveCount(1);
    }

    [Fact]
    public async Task OnTickAsync_WhenConsecutiveFailures_ShouldIncreaseDelay()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        f.WorkerClusterOps
            .Setup(x => x.ExecWithRetryAsync(
                It.IsAny<Action<IWorkerClusterOperations>>(),
                It.IsAny<int>(),
                It.IsAny<int>()))
            .Throws(new InvalidOperationException("simulated failure"));

        var runner = CreateRunner(f);

        f.JobsDispatcher.PendingRecurQueue.Add(PendingSchedule(f.ClusterId, TestBucketId));
        var result1 = await runner.OnTickAsync(CancellationToken.None);

        f.JobsDispatcher.PendingRecurQueue.Add(PendingSchedule(f.ClusterId, TestBucketId));
        var result2 = await runner.OnTickAsync(CancellationToken.None);

        result2.Delay.Should().BeGreaterThanOrEqualTo(result1.Delay);
    }
}
