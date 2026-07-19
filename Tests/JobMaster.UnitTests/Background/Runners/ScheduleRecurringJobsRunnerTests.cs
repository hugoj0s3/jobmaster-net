using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="ScheduleRecurringJobsRunner"/>.
/// Covers: skip when the cluster is not in Active mode, lock contention path, skip when no
/// eligible schedules are found, and successful planning when Active schedules with expired
/// coverage are present.
/// </summary>
public class ScheduleRecurringJobsRunnerTests
{
    private static ClusterConfigurationModel ActiveClusterConfig()
        => new("test") { ClusterMode = ClusterMode.Active };

    private static ClusterConfigurationModel MigratingClusterConfig()
        => new("test") { ClusterMode = ClusterMode.Migrating };

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenClusterModeIsNotActive_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = MigratingClusterConfig();

        var runner = new ScheduleRecurringJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        f.RecurringSchedulePlanner.PlannedSchedules.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenClusterConfigIsNull_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = null; // No config present.

        var runner = new ScheduleRecurringJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenLockAlreadyHeld_ShouldReturnLocked()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ActiveClusterConfig();
        f.Locker.BlockAllLocks = true;
        // Add a schedule so the count > 0 and the runner proceeds past the scan-plan stage.
        f.RecurringSchedulesService.Schedules.Add(RunnerFixture.ActiveSchedule(f.ClusterId));

        var runner = new ScheduleRecurringJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
        f.RecurringSchedulePlanner.PlannedSchedules.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenNoEligibleSchedules_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ActiveClusterConfig();
        // No schedules → AcquireAndFetchAsync returns empty list → Skipped.

        var runner = new ScheduleRecurringJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        f.RecurringSchedulePlanner.PlannedSchedules.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenActiveSchedulesExist_ShouldPlanThemAndReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ActiveClusterConfig();

        var schedule = RunnerFixture.ActiveSchedule(f.ClusterId);
        f.RecurringSchedulesService.Schedules.Add(schedule);

        var runner = new ScheduleRecurringJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.RecurringSchedulePlanner.PlannedSchedules.Should().ContainSingle(s => s.Id == schedule.Id);
    }

    [Fact]
    public async Task OnTickAsync_WhenMultipleSchedulesExist_ShouldPlanAllOfThem()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ActiveClusterConfig();

        var schedule1 = RunnerFixture.ActiveSchedule(f.ClusterId);
        var schedule2 = RunnerFixture.ActiveSchedule(f.ClusterId);
        f.RecurringSchedulesService.Schedules.Add(schedule1);
        f.RecurringSchedulesService.Schedules.Add(schedule2);

        var runner = new ScheduleRecurringJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.RecurringSchedulePlanner.PlannedSchedules.Should().HaveCount(2);
    }
}
