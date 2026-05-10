using FluentAssertions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="RecentlyInsertedScheduleRecurringJobsRunner"/>.
/// Covers: skip when the queue is empty, lock contention path with re-enqueueing, and
/// successful planning when newly inserted schedule IDs are present in the queue.
/// </summary>
public class RecentlyInsertedScheduleRecurringJobsRunnerTests
{
    public RecentlyInsertedScheduleRecurringJobsRunnerTests()
    {
        // CountWorkersAsync is called during every tick; set it up once here.
    }

    private static RunnerFixture CreateFixture()
    {
        var f = RunnerFixture.Create();
        // RecentlyInsertedScheduleRecurringJobsRunner calls CountWorkersAsync (not
        // CountActiveCoordinatorWorkersAsync), so we need to wire that up.
        f.WorkerClusterOps
            .Setup(x => x.CountWorkersAsync())
            .ReturnsAsync(1);
        return f;
    }

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenQueueIsEmpty_ShouldReturnSkipped()
    {
        var f = CreateFixture();
        // Queue is empty — nothing to plan.

        var runner = new RecentlyInsertedScheduleRecurringJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        f.RecurringSchedulePlanner.PlannedSchedules.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenLockAlreadyHeld_ShouldRequeueIdsAndReturnLocked()
    {
        var f = CreateFixture();
        f.Locker.BlockAllLocks = true;

        var scheduleId = Guid.NewGuid();
        f.RecentlyInsertedQueue.Queue.Add(scheduleId);

        var runner = new RecentlyInsertedScheduleRecurringJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
        // IDs must be re-enqueued so they are not lost.
        f.RecentlyInsertedQueue.Queue.Should().Contain(scheduleId);
        f.RecurringSchedulePlanner.PlannedSchedules.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenScheduleIdInQueue_ShouldPlanItAndReturnSuccess()
    {
        var f = CreateFixture();

        var schedule = RunnerFixture.ActiveSchedule(f.ClusterId);
        f.RecurringSchedulesService.Schedules.Add(schedule);
        f.RecentlyInsertedQueue.Queue.Add(schedule.Id);

        var runner = new RecentlyInsertedScheduleRecurringJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.RecurringSchedulePlanner.PlannedSchedules.Should().ContainSingle(s => s.Id == schedule.Id);
        // Queue should be empty after dequeue.
        f.RecentlyInsertedQueue.Queue.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenMultipleSchedulesInQueue_ShouldPlanAllOfThem()
    {
        var f = CreateFixture();

        var schedule1 = RunnerFixture.ActiveSchedule(f.ClusterId);
        var schedule2 = RunnerFixture.ActiveSchedule(f.ClusterId);
        f.RecurringSchedulesService.Schedules.Add(schedule1);
        f.RecurringSchedulesService.Schedules.Add(schedule2);
        f.RecentlyInsertedQueue.Queue.Add(schedule1.Id);
        f.RecentlyInsertedQueue.Queue.Add(schedule2.Id);

        var runner = new RecentlyInsertedScheduleRecurringJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.RecurringSchedulePlanner.PlannedSchedules.Should().HaveCount(2);
    }

    [Fact]
    public async Task OnTickAsync_WhenIdNotFoundInService_ShouldStillReturnSuccess()
    {
        var f = CreateFixture();

        // ID in queue but not in the service — fetch returns nothing; runner still succeeds.
        f.RecentlyInsertedQueue.Queue.Add(Guid.NewGuid());

        var runner = new RecentlyInsertedScheduleRecurringJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.RecurringSchedulePlanner.PlannedSchedules.Should().BeEmpty();
    }
}
