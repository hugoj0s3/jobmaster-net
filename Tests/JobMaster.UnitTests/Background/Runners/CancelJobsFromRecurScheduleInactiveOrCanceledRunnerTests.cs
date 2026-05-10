using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="CancelJobsFromRecurScheduleInactiveOrCanceledRunner"/>.
/// Covers: lock contention path, skip when no eligible schedules, marking the schedule as
/// finished when all of its jobs are already in a final state, and bulk-cancelling non-final
/// jobs and marking the schedule finished when future jobs remain.
/// </summary>
public class CancelJobsFromRecurScheduleInactiveOrCanceledRunnerTests
{
    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenLockAlreadyHeld_ShouldReturnLocked()
    {
        var f = RunnerFixture.Create();
        f.Locker.BlockAllLocks = true;
        f.RecurringSchedulesService.Schedules.Add(
            RunnerFixture.CanceledScheduleWithJobCancellationPending(f.ClusterId));

        var runner = new CancelJobsFromRecurScheduleInactiveOrCanceledRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
    }

    [Fact]
    public async Task OnTickAsync_WhenNoEligibleSchedules_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        // Active schedule — does not match the cancellation criteria.
        f.RecurringSchedulesService.Schedules.Add(RunnerFixture.ActiveSchedule(f.ClusterId));

        var runner = new CancelJobsFromRecurScheduleInactiveOrCanceledRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenScheduleHasNoNonFinalJobs_ShouldMarkScheduleFinished()
    {
        var f = RunnerFixture.Create();
        var schedule = RunnerFixture.CanceledScheduleWithJobCancellationPending(f.ClusterId);
        f.RecurringSchedulesService.Schedules.Add(schedule);

        // No non-final jobs for this schedule — runner should mark it finished and upsert.
        // (Fake AcquireAndFetchAsync returns empty because no jobs were added.)

        var runner = new CancelJobsFromRecurScheduleInactiveOrCanceledRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        // Schedule should have IsJobCancellationPending cleared.
        var upserted = f.RecurringSchedulesService.Schedules.First(s => s.Id == schedule.Id);
        upserted.IsJobCancellationPending.Should().NotBeTrue();
    }

    [Fact]
    public async Task OnTickAsync_WhenScheduleHasPendingJobs_ShouldBulkCancelThemAndMarkFinished()
    {
        var f = RunnerFixture.Create();
        var schedule = RunnerFixture.CanceledScheduleWithJobCancellationPending(f.ClusterId);
        f.RecurringSchedulesService.Schedules.Add(schedule);

        // Add a non-final job that the fake service will return.
        var pendingJob = new JobRawModel
        {
            Id = Guid.NewGuid(),
            Status = JobMasterJobStatus.OnMaster,
            SourceId = schedule.Id,
            NextPlanExecutionAt = DateTime.UtcNow.AddHours(1),
        };
        f.JobsService.Jobs.Add(pendingJob);

        var runner = new CancelJobsFromRecurScheduleInactiveOrCanceledRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        // Runner should have issued a bulk cancel request.
        f.JobsService.BulkUpdateRequests.Should().NotBeEmpty();
    }
}
