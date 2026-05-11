using FluentAssertions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Background.Runners.CleanUpData;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="DeleteOldInactiveRecurringSchedulesRunner"/>.
/// Covers: skip when no config or TTL is null, lock-contention skip, purging terminated
/// schedules older than the TTL while preserving active and recently terminated ones.
/// </summary>
public class DeleteOldInactiveRecurringSchedulesRunnerTests
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private static ClusterConfigurationModel ConfigWithTtl(TimeSpan ttl)
        => new("test-cluster") { DataRetentionTtl = ttl };

    private static RecurringScheduleRawModel TerminatedSchedule(DateTime terminatedAt)
        => new()
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            TerminatedAt = terminatedAt,
        };

    private static RecurringScheduleRawModel ActiveSchedule()
        => new()
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            TerminatedAt = null,
        };

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenNoClusterConfig_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        // Config is null by default.

        var runner = new DeleteOldInactiveRecurringSchedulesRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenDataRetentionTtlIsNull_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = new ClusterConfigurationModel("test-cluster")
        {
            DataRetentionTtl = null,
        };

        var runner = new DeleteOldInactiveRecurringSchedulesRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenLockerTaken_ShouldReturnLocked()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ConfigWithTtl(TimeSpan.FromDays(7));
        f.Locker.BlockAllLocks = true;
        f.RecurringSchedules.Schedules.Add(TerminatedSchedule(DateTime.UtcNow.AddDays(-30)));

        var runner = new DeleteOldInactiveRecurringSchedulesRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
        f.RecurringSchedules.Schedules.Should().HaveCount(1); // not deleted
    }

    [Fact]
    public async Task OnTickAsync_WhenTerminatedSchedulesOlderThanTtl_ShouldPurgeThem()
    {
        var f = RunnerFixture.Create();
        var ttl = TimeSpan.FromDays(7);
        f.ClusterConfig.Config = ConfigWithTtl(ttl);

        // Two old terminated schedules — should be purged.
        f.RecurringSchedules.Schedules.Add(TerminatedSchedule(DateTime.UtcNow.AddDays(-30)));
        f.RecurringSchedules.Schedules.Add(TerminatedSchedule(DateTime.UtcNow.AddDays(-10)));

        // One active schedule — should NOT be purged.
        f.RecurringSchedules.Schedules.Add(ActiveSchedule());

        // One recently terminated schedule — within TTL, should NOT be purged.
        f.RecurringSchedules.Schedules.Add(TerminatedSchedule(DateTime.UtcNow.AddDays(-1)));

        var runner = new DeleteOldInactiveRecurringSchedulesRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.RecurringSchedules.Schedules.Should().HaveCount(2); // active + recently terminated
        f.RecurringSchedules.Schedules.Should().NotContain(s =>
            s.TerminatedAt.HasValue && s.TerminatedAt.Value < DateTime.UtcNow.AddDays(-7));
    }

    [Fact]
    public async Task OnTickAsync_WhenNoSchedulesToDelete_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ConfigWithTtl(TimeSpan.FromDays(7));
        f.RecurringSchedules.Schedules.Add(ActiveSchedule());

        var runner = new DeleteOldInactiveRecurringSchedulesRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.RecurringSchedules.Schedules.Should().HaveCount(1);
    }
}
