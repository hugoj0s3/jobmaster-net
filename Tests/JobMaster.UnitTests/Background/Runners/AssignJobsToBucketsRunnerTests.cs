using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="AssignJobsToBucketsRunner"/>.
/// Covers: skip when stop is requested, skip when the cluster is not in Active mode,
/// lock contention path, skip when no OnMaster jobs are present, and successful assignment
/// when both an OnMaster job and an available bucket exist.
/// </summary>
public class AssignJobsToBucketsRunnerTests
{
    private static ClusterConfigurationModel ActiveClusterConfig()
        => new("test") { ClusterMode = ClusterMode.Active };

    private static JobRawModel OnMasterJob()
        => new()
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = JobMasterJobStatus.OnMaster,
            NextPlanExecutionAt = DateTime.UtcNow,
        };

    // ── OnTickAsync ────────────────────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenStopRequested_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.Worker.SetupGet(x => x.StopRequested).Returns(true);
        f.ClusterConfig.Config = ActiveClusterConfig();

        var runner = new AssignJobsToBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenClusterModeIsNotActive_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = new ClusterConfigurationModel("test") { ClusterMode = ClusterMode.Passive };

        var runner = new AssignJobsToBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenClusterConfigIsNull_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = null;

        var runner = new AssignJobsToBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenLockAlreadyHeld_ShouldReturnLocked()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ActiveClusterConfig();
        f.Locker.BlockAllLocks = true;
        // Add a job so the count > 0 and runner proceeds to the lock step.
        f.JobsService.Jobs.Add(OnMasterJob());

        var runner = new AssignJobsToBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Locked);
    }

    [Fact]
    public async Task OnTickAsync_WhenNoOnMasterJobs_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ActiveClusterConfig();
        // No jobs — AcquireAndFetchAsync returns empty.

        var runner = new AssignJobsToBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenJobExistsAndBucketAvailable_ShouldAssignJobAndReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ActiveClusterConfig();

        var job = OnMasterJob();
        f.JobsService.Jobs.Add(job);

        // Provide an active bucket for SelectBucketAsync to return.
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, "target-bucket", f.WorkerId));

        var runner = new AssignJobsToBucketsRunner(f.Worker.Object);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        // The job should have been bulk-updated (i.e. assigned to a bucket).
        f.JobsService.Jobs.Should().Contain(j => j.Id == job.Id && j.BucketId == "target-bucket");
    }
}
