using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Background.Runners.CleanUpData;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="DeleteOldFinalJobsRunner"/>.
/// Covers: skip when no config or TTL is null, lock-contention skip, purging finalized jobs
/// older than the TTL while preserving recent ones, and no-op when nothing is eligible.
/// </summary>
public class DeleteOldFinalJobsRunnerTests
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private static ClusterConfigurationModel ConfigWithTtl(TimeSpan ttl)
        => new("test-cluster") { DataRetentionTtl = ttl };

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenNoClusterConfig_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        // Config is null by default in FakeClusterConfigService.

        var runner = new DeleteOldFinalJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        f.JobsRepository.Jobs.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenDataRetentionTtlIsNull_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = new ClusterConfigurationModel("test-cluster")
        {
            DataRetentionTtl = null,
        };

        var runner = new DeleteOldFinalJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenLockerTaken_ShouldReturnLocked()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ConfigWithTtl(TimeSpan.FromDays(7));
        f.Locker.BlockAllLocks = true;

        // Add a job that would otherwise be purged.
        f.JobsRepository.Jobs.Add(RunnerFixture.FinalizedJob(DateTime.UtcNow.AddDays(-30)));

        var runner = new DeleteOldFinalJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
        f.JobsRepository.Jobs.Should().HaveCount(1); // not deleted
    }

    [Fact]
    public async Task OnTickAsync_WhenFinalJobsOlderThanTtl_ShouldPurgeThem()
    {
        var f = RunnerFixture.Create();
        var ttl = TimeSpan.FromDays(7);
        f.ClusterConfig.Config = ConfigWithTtl(ttl);

        // Two old finalized jobs — both should be purged.
        f.JobsRepository.Jobs.Add(RunnerFixture.FinalizedJob(DateTime.UtcNow.AddDays(-30)));
        f.JobsRepository.Jobs.Add(RunnerFixture.FinalizedJob(DateTime.UtcNow.AddDays(-10)));

        // One recent finalized job — within TTL, should NOT be purged.
        f.JobsRepository.Jobs.Add(RunnerFixture.FinalizedJob(DateTime.UtcNow.AddDays(-1)));

        var runner = new DeleteOldFinalJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.JobsRepository.Jobs.Should().HaveCount(1);
        f.JobsRepository.Jobs.Single().FinalizedAt.Should().BeCloseTo(DateTime.UtcNow.AddDays(-1), TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task OnTickAsync_WhenNoJobsToDelete_ShouldReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ConfigWithTtl(TimeSpan.FromDays(7));
        // No jobs at all.

        var runner = new DeleteOldFinalJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
    }
}
