using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background.SavePendingRecurringSchedules;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

public class RecurringScheduleSavePendingOperationTests
{
    [Fact]
    public async Task SaveRecurringScheduleAsync_WhenSaved_EnqueuesForFastPlanning()
    {
        var f = RunnerFixture.Create();
        var schedule = RunnerFixture.InactiveSchedule(f.ClusterId);

        await new RecurringScheduleSavePendingOperation(f.Worker.Object).SaveRecurringScheduleAsync(schedule);

        schedule.Status.Should().Be(RecurringScheduleStatus.Active);
        f.WorkerClusterOps.Verify(x => x.Upsert(schedule), Times.Once);
        f.RecentlyInsertedQueue.Queue.Should().ContainSingle().Which.Should().Be(schedule.Id);
    }

    [Fact]
    public async Task SaveRecurringScheduleAsync_WhenAlreadyOnMaster_DoesNotThrow_AndStillEnqueues()
    {
        // e.g. ConfirmOnMasterAsync inserted it first: still planned through the fast path.
        var f = RunnerFixture.Create();
        var schedule = RunnerFixture.InactiveSchedule(f.ClusterId);
        f.WorkerClusterOps
            .Setup(x => x.Upsert(It.IsAny<RecurringScheduleRawModel>()))
            .Throws(new JobMasterDuplicationException(schedule.Id, "RecurringSchedule", new Exception("dup")));

        await new RecurringScheduleSavePendingOperation(f.Worker.Object).SaveRecurringScheduleAsync(schedule);

        f.RecentlyInsertedQueue.Queue.Should().ContainSingle().Which.Should().Be(schedule.Id);
    }
}
