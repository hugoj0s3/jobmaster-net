using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Background.Runners.DrainRunners;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// <see cref="PollingDrainSavePendingJobsRunner"/>: a draining bucket's pending-save jobs are all held on master
/// (never dispatched, even when due now), and their messages acked once on master.
/// </summary>
public class PollingDrainSavePendingJobsRunnerTests
{
    private const string TestBucketId = "drain-bucket";

    private static PollingDrainSavePendingJobsRunner CreateRunner(RunnerFixture f)
    {
        var runner = new PollingDrainSavePendingJobsRunner(f.Worker.Object);
        runner.DefineBucketId(TestBucketId);
        return runner;
    }

    private static JobRawModel DueNowJob()
        => new()
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = JobMasterJobStatus.PendingSave,
            BucketId = TestBucketId,
            NextPlanExecutionAt = DateTime.UtcNow,
        };

    [Fact]
    public async Task OnTickAsync_WhenBucketIsNotDraining_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        f.JobsDispatcher.PendingQueue.Add(DueNowJob());

        var result = await CreateRunner(f).OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
        f.JobsDispatcher.PendingQueue.Should().HaveCount(1);
    }

    [Fact]
    public async Task OnTickAsync_WhenDraining_HoldsEveryJobOnMaster_AcksAndDispatchesNothing()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.DrainingBucket(f.ClusterId, TestBucketId));
        var job = DueNowJob();
        f.JobsDispatcher.PendingQueue.Add(job);

        var result = await CreateRunner(f).OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.JobsService.Jobs.Should().ContainSingle(j => j.Id == job.Id && j.Status == JobMasterJobStatus.OnMaster);
        f.JobsDispatcher.AckedKeys.Should().HaveCount(1);
        f.JobsDispatcher.LeasedJobs.Should().BeEmpty();
        f.JobsDispatcher.PublishedForProcessing.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenJobIsNotConfirmedOnMaster_ReturnsFailed_AndLeavesItsMessageLeased()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.DrainingBucket(f.ClusterId, TestBucketId));
        var job = DueNowJob();
        f.JobsService.UnknownOnInsert.Add(job.Id);
        f.JobsDispatcher.PendingQueue.Add(job);

        var result = await CreateRunner(f).OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Failed);
        f.JobsDispatcher.AckedKeys.Should().BeEmpty();
        f.JobsDispatcher.LeasedJobs.Values.Should().ContainSingle(j => j.Id == job.Id);
    }
}
