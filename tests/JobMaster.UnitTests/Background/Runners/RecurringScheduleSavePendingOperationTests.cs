using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background.SavePendingRecurringSchedules;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// <see cref="RecurringScheduleSavePendingOperation.SaveAsync"/> (the NATS save runners): saved means on master, and a
/// saved schedule is queued for fast planning.
/// </summary>
public class RecurringScheduleSavePendingOperationTests
{
    [Fact]
    public async Task SaveAsync_WhenSaved_SavesItActive_AndEnqueuesForFastPlanning()
    {
        var f = RunnerFixture.Create();
        var schedule = RunnerFixture.InactiveSchedule(f.ClusterId);

        (await new RecurringScheduleSavePendingOperation(f.Worker.Object).SaveAsync(schedule)).Should().BeTrue();

        f.RecurringSchedulesService.Schedules.Should().ContainSingle(s => s.Id == schedule.Id && s.Status == RecurringScheduleStatus.Active);
        f.RecentlyInsertedQueue.Queue.Should().ContainSingle().Which.Should().Be(schedule.Id);
    }

    [Fact]
    public async Task SaveAsync_WhenAlreadyOnMaster_KeepsMastersCopy_AndStillEnqueues()
    {
        // e.g. ConfirmOnMasterAsync inserted it first, or it was cancelled since: master's copy wins.
        var f = RunnerFixture.Create();
        var schedule = RunnerFixture.InactiveSchedule(f.ClusterId);
        f.RecurringSchedulesService.Schedules.Add(new RecurringScheduleRawModel(f.ClusterId) { Id = schedule.Id, Status = RecurringScheduleStatus.Canceled });

        (await new RecurringScheduleSavePendingOperation(f.Worker.Object).SaveAsync(schedule)).Should().BeTrue();

        f.RecurringSchedulesService.Schedules.Should().ContainSingle(s => s.Id == schedule.Id && s.Status == RecurringScheduleStatus.Canceled);
        f.RecentlyInsertedQueue.Queue.Should().ContainSingle().Which.Should().Be(schedule.Id);
    }

    [Fact]
    public async Task SaveAsync_WhenNotConfirmedOnMaster_IsNotSaved_AndNotEnqueued()
    {
        var f = RunnerFixture.Create();
        var schedule = RunnerFixture.InactiveSchedule(f.ClusterId);
        f.RecurringSchedulesService.UnknownOnInsert.Add(schedule.Id);

        (await new RecurringScheduleSavePendingOperation(f.Worker.Object).SaveAsync(schedule)).Should().BeFalse();

        f.RecentlyInsertedQueue.Queue.Should().BeEmpty();
    }
}
