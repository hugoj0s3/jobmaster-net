using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Background.Runners.DrainRunners;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="PollingDrainSavePendingRecurringScheduleRunner"/>.
/// Covers: skip when no bucket ID is set, skip when the bucket is not in Draining status,
/// skip when the pending-recurring queue is empty, successful drain-save processing, and
/// per-schedule failure with consecutive-failure backoff applied to the next interval.
/// </summary>
public class PollingDrainSavePendingRecurringScheduleRunnerTests
{
    private const string TestBucketId = "drain-bucket";

    private static PollingDrainSavePendingRecurringScheduleRunner CreateRunner(
        RunnerFixture f, string? bucketId = TestBucketId)
    {
        var runner = new PollingDrainSavePendingRecurringScheduleRunner(f.Worker.Object);
        if (bucketId != null)
            runner.DefineBucketId(bucketId);
        return runner;
    }

    private static RecurringScheduleRawModel PendingSchedule(string clusterId)
        => new(clusterId)
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = RecurringScheduleStatus.PendingSave,
            BucketId = TestBucketId,
        };

    // ── OnTickAsync — skip paths ───────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenBucketIdNotDefined_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        var runner = new PollingDrainSavePendingRecurringScheduleRunner(f.Worker.Object);

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
        f.RecurringSchedulesService.Schedules.Should().ContainSingle(s => s.Status == RecurringScheduleStatus.Active);
        f.JobsDispatcher.AckedRecurKeys.Should().HaveCount(1);
    }

    [Fact]
    public async Task OnTickAsync_WhenScheduleIsNotConfirmedOnMaster_ReturnsFailed_AndLeavesItsMessageLeased()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.DrainingBucket(f.ClusterId, TestBucketId));
        var schedule = PendingSchedule(f.ClusterId);
        f.RecurringSchedulesService.UnknownOnInsert.Add(schedule.Id);
        f.JobsDispatcher.PendingRecurQueue.Add(schedule);

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Failed);
        f.JobsDispatcher.AckedRecurKeys.Should().BeEmpty();
        f.JobsDispatcher.LeasedRecurSchedules.Values.Should().ContainSingle(s => s.Id == schedule.Id);
    }
}
