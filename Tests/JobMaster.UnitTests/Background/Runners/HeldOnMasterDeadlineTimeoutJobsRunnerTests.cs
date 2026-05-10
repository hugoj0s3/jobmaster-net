using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="HeldOnMasterDeadlineTimeoutJobsRunner"/>.
/// Covers: lock-contention skip, no-op when no overdue jobs, exclusion of active-bucket
/// jobs, bulk-marking eligible jobs as HeldOnMaster, and filtering of final-status jobs.
/// </summary>
public class HeldOnMasterDeadlineTimeoutJobsRunnerTests
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private static JobRawModel OverdueJob(string? bucketId = null)
        => new()
        {
            Id = Guid.NewGuid(),
            Status = JobMasterJobStatus.OnMaster,
            ProcessDeadline = DateTime.UtcNow.AddMinutes(-10),
            BucketId = bucketId,
        };

    private static JobRawModel FinalJob(string? bucketId = null)
        => new()
        {
            Id = Guid.NewGuid(),
            Status = JobMasterJobStatus.Succeeded,
            ProcessDeadline = DateTime.UtcNow.AddMinutes(-5),
            BucketId = bucketId,
        };

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenLockerTaken_ShouldReturnLocked()
    {
        var f = RunnerFixture.Create();
        f.Locker.BlockAllLocks = true;
        f.JobsService.Jobs.Add(OverdueJob());

        var runner = new HeldOnMasterDeadlineTimeoutJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
        f.JobsService.BulkUpdateRequests.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenNoJobsPastDeadline_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();

        // Jobs with future deadlines are not yet overdue.
        f.JobsService.Jobs.Add(new JobRawModel
        {
            Id = Guid.NewGuid(),
            Status = JobMasterJobStatus.OnMaster,
            ProcessDeadline = DateTime.UtcNow.AddHours(1),
        });

        var runner = new HeldOnMasterDeadlineTimeoutJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        f.JobsService.BulkUpdateRequests.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenJobsAreInActiveBucket_ShouldExcludeThemFromUpdate()
    {
        var f = RunnerFixture.Create();
        var activeBucketId = "active-bucket";
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, activeBucketId));

        // This job is overdue but lives in an active bucket — the runner should exclude it.
        f.JobsService.Jobs.Add(OverdueJob(activeBucketId));

        var runner = new HeldOnMasterDeadlineTimeoutJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        f.JobsService.BulkUpdateRequests.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenEligibleJobsExist_ShouldMarkThemAsHeldOnMaster()
    {
        var f = RunnerFixture.Create();

        // Two overdue jobs not in any active bucket.
        var job1 = OverdueJob();
        var job2 = OverdueJob();
        f.JobsService.Jobs.Add(job1);
        f.JobsService.Jobs.Add(job2);

        var runner = new HeldOnMasterDeadlineTimeoutJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.JobsService.BulkUpdateRequests.Should().NotBeEmpty();

        var updatedIds = f.JobsService.BulkUpdateRequests.SelectMany(r => r.JobIds).ToList();
        updatedIds.Should().Contain(job1.Id);
        updatedIds.Should().Contain(job2.Id);
    }

    [Fact]
    public async Task OnTickAsync_WhenJobsHaveFinalStatus_ShouldSkipThem()
    {
        var f = RunnerFixture.Create();

        // Final-status jobs (Succeeded, Failed, Cancelled, Aborted) must not be updated.
        f.JobsService.Jobs.Add(FinalJob());
        f.JobsService.Jobs.Add(FinalJob());

        var runner = new HeldOnMasterDeadlineTimeoutJobsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        // No eligible jobs → no bulk update.
        f.JobsService.BulkUpdateRequests.Should().BeEmpty();
        result.Status.Should().NotBe(TicketResultStatus.Failed);
    }
}
