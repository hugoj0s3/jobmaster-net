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
    public async Task OnTickAsync_WhenPendingSchedulesExist_SavesThemActiveAndAcks()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        var schedule = PendingSchedule(f.ClusterId, TestBucketId);
        f.JobsDispatcher.PendingRecurQueue.Add(schedule);

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.RecurringSchedulesService.Schedules.Should().ContainSingle(s => s.Id == schedule.Id && s.Status == RecurringScheduleStatus.Active);
        f.JobsDispatcher.AckedRecurKeys.Should().HaveCount(1);
        f.JobsDispatcher.LeasedRecurSchedules.Should().BeEmpty();
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
    public async Task OnTickAsync_WhenScheduleIsAlreadyOnMaster_KeepsMastersCopyAndAcks()
    {
        // Master's copy wins: a re-leased message must not bring back a schedule cancelled on master.
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        var schedule = PendingSchedule(f.ClusterId, TestBucketId);
        f.RecurringSchedulesService.Schedules.Add(new RecurringScheduleRawModel(f.ClusterId) { Id = schedule.Id, Status = RecurringScheduleStatus.Canceled });
        f.JobsDispatcher.PendingRecurQueue.Add(schedule);

        var runner = CreateRunner(f);
        await runner.OnTickAsync(CancellationToken.None);

        f.RecurringSchedulesService.Schedules.Should().ContainSingle(s => s.Id == schedule.Id && s.Status == RecurringScheduleStatus.Canceled);
        f.JobsDispatcher.AckedRecurKeys.Should().HaveCount(1);
    }

    [Fact]
    public async Task OnTickAsync_WhenScheduleIsNotConfirmedOnMaster_ReturnsFailed_AndLeavesItsMessageLeased()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        var schedule = PendingSchedule(f.ClusterId, TestBucketId);
        f.RecurringSchedulesService.UnknownOnInsert.Add(schedule.Id);
        f.JobsDispatcher.PendingRecurQueue.Add(schedule);

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Failed);
        f.JobsDispatcher.AckedRecurKeys.Should().BeEmpty();
        f.JobsDispatcher.LeasedRecurSchedules.Values.Should().ContainSingle(s => s.Id == schedule.Id);
    }
}
