using FluentAssertions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Background.Runners.CleanUpData;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="DeleteOldLogsRunner"/>.
/// Covers: skip when no config or TTL is null, lock-contention skip, deleting log records
/// older than the TTL while preserving recent ones, and no-op when nothing is eligible.
/// </summary>
public class DeleteOldLogsRunnerTests
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private static ClusterConfigurationModel ConfigWithTtl(TimeSpan ttl)
        => new("test-cluster") { DataRetentionTtl = ttl };

    /// <summary>Creates a log GenericRecordEntry with a specific CreatedAt timestamp.</summary>
    private static GenericRecordEntry LogRecord(string clusterId, DateTime createdAt)
    {
        var record = GenericRecordEntry.Create(
            clusterId,
            MasterGenericRecordGroupIds.Log,
            entryId: Guid.NewGuid().ToString(),
            obj: new { });

        record.CreatedAt = createdAt;
        return record;
    }

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenNoClusterConfig_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        // Config is null by default.

        var runner = new DeleteOldLogsRunner(f.Worker.Object);
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

        var runner = new DeleteOldLogsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenLockerTaken_ShouldReturnLocked()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ConfigWithTtl(TimeSpan.FromDays(7));
        f.Locker.BlockAllLocks = true;
        f.GenericRecords.Records.Add(LogRecord(f.ClusterId, DateTime.UtcNow.AddDays(-30)));

        var runner = new DeleteOldLogsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
        f.GenericRecords.Records.Should().HaveCount(1); // not deleted
    }

    [Fact]
    public async Task OnTickAsync_WhenLogRecordsOlderThanTtl_ShouldDeleteThem()
    {
        var f = RunnerFixture.Create();
        var ttl = TimeSpan.FromDays(7);
        f.ClusterConfig.Config = ConfigWithTtl(ttl);

        // Two old log records — should be deleted.
        f.GenericRecords.Records.Add(LogRecord(f.ClusterId, DateTime.UtcNow.AddDays(-30)));
        f.GenericRecords.Records.Add(LogRecord(f.ClusterId, DateTime.UtcNow.AddDays(-10)));

        // One recent log record — within TTL, should NOT be deleted.
        f.GenericRecords.Records.Add(LogRecord(f.ClusterId, DateTime.UtcNow.AddDays(-1)));

        var runner = new DeleteOldLogsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.GenericRecords.Records.Should().HaveCount(1);
        f.GenericRecords.Records.Single().CreatedAt
            .Should().BeCloseTo(DateTime.UtcNow.AddDays(-1), TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task OnTickAsync_WhenNoRecordsToDelete_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ConfigWithTtl(TimeSpan.FromDays(7));
        // No records at all.

        var runner = new DeleteOldLogsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
    }
}
