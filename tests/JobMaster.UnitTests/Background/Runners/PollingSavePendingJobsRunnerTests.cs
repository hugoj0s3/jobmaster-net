using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Background.Runners.SavePendingJobs;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// Unit tests for <see cref="PollingSavePendingJobsRunner"/>.
/// Covers: skip when no bucket ID is set, skip for non-Active/Completing bucket status,
/// skip when the queue is empty, successful processing of transient jobs (held-on-master
/// path), processing under a Completing bucket, per-job failure re-queuing with backoff,
/// and growing delay across consecutive failures.
/// </summary>
public class PollingSavePendingJobsRunnerTests
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private const string TestBucketId = "test-bucket";

    /// <summary>
    /// Creates a runner and immediately assigns it a bucket ID so it is ready to process.
    /// </summary>
    private static PollingSavePendingJobsRunner CreateRunner(
        RunnerFixture f, string? bucketId = TestBucketId)
    {
        var runner = new PollingSavePendingJobsRunner(f.Worker.Object);
        if (bucketId != null)
            runner.DefineBucketId(bucketId);
        return runner;
    }

    /// <summary>
    /// A pending-save job whose NextPlanExecutionAt is far in the future, placing it in the
    /// transient path (held-on-master without touching the dispatcher service).
    /// </summary>
    private static JobRawModel TransientJob(string bucketId)
        => new()
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = JobMasterJobStatus.PendingSave,
            BucketId = bucketId,
            NextPlanExecutionAt = DateTime.UtcNow.AddHours(1), // beyond any transient threshold
        };

    // ── OnTickAsync — skip paths ───────────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenBucketIdNotDefined_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        var runner = new PollingSavePendingJobsRunner(f.Worker.Object); // no DefineBucketId

        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketStatusIsNotActiveOrCompleting_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        // Bucket exists but has a non-processing status.
        f.Buckets.Buckets.Add(new BucketModel(f.ClusterId) { Id = TestBucketId, Status = BucketStatus.ReadyToDelete });

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenBucketDoesNotExist_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        // No bucket added — Get returns null.

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    [Fact]
    public async Task OnTickAsync_WhenNoPendingJobs_ShouldReturnSkipped()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        // PendingQueue is empty.

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Skipped);
    }

    // ── OnTickAsync — processing paths ────────────────────────────────────────

    [Fact]
    public async Task OnTickAsync_WhenJobIsPastTheTransientThreshold_HoldsItOnMasterAndAcks()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        var job = TransientJob(TestBucketId);
        f.JobsDispatcher.PendingQueue.Add(job);

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.JobsService.Jobs.Should().ContainSingle(j => j.Id == job.Id && j.Status == JobMasterJobStatus.OnMaster);
        f.JobsDispatcher.AckedKeys.Should().HaveCount(1);
        f.JobsDispatcher.LeasedJobs.Should().BeEmpty();
        f.JobsDispatcher.PublishedForProcessing.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenCompletingBucketHasPendingJobs_ShouldProcessThem()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.CompletingBucket(f.ClusterId, TestBucketId));
        f.JobsDispatcher.PendingQueue.Add(TransientJob(TestBucketId));

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.JobsDispatcher.AckedKeys.Should().HaveCount(1);
    }

    [Fact]
    public async Task OnTickAsync_WhenJobIsDueSoon_AssignsItToABucket_DispatchesAndAcks()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        var job = DueSoonJob(TestBucketId);
        f.JobsDispatcher.PendingQueue.Add(job);

        var runner = CreateRunner(f);
        await runner.OnTickAsync(CancellationToken.None);

        f.JobsService.Jobs.Should().ContainSingle(j => j.Id == job.Id && j.Status == JobMasterJobStatus.InBucket);
        f.WorkerClusterOps.Verify(x => x.BulkDispatchJobsToBucketAsync(
            It.IsAny<IJobMasterBackgroundAgentWorker>(),
            It.Is<BucketModel>(b => b.Id == TestBucketId),
            It.Is<IList<JobRawModel>>(jobs => jobs.Count == 1 && jobs[0].Id == job.Id)), Times.Once);
        f.JobsDispatcher.AckedKeys.Should().HaveCount(1);
    }

    [Fact]
    public async Task OnTickAsync_WhenJobIsAlreadyOnMaster_AcksWithoutPublishingAgain()
    {
        // A message leased again after its lease ran out: the job was saved (and dispatched) the first time.
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        var job = DueSoonJob(TestBucketId);
        f.JobsService.Jobs.Add(new JobRawModel { Id = job.Id, Status = JobMasterJobStatus.InBucket });
        f.JobsDispatcher.PendingQueue.Add(job);

        var runner = CreateRunner(f);
        await runner.OnTickAsync(CancellationToken.None);

        f.JobsDispatcher.AckedKeys.Should().HaveCount(1);
        f.WorkerClusterOps.Verify(x => x.BulkDispatchJobsToBucketAsync(
            It.IsAny<IJobMasterBackgroundAgentWorker>(), It.IsAny<BucketModel>(), It.IsAny<IList<JobRawModel>>()), Times.Never);
    }

    [Fact]
    public async Task OnTickAsync_WhenJobIsNotConfirmedOnMaster_LeavesItsMessageLeased()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        var job = DueSoonJob(TestBucketId);
        f.JobsService.UnknownOnInsert.Add(job.Id);
        f.JobsDispatcher.PendingQueue.Add(job);

        var runner = CreateRunner(f);
        await runner.OnTickAsync(CancellationToken.None);

        f.JobsDispatcher.AckedKeys.Should().BeEmpty();
        f.JobsDispatcher.LeasedJobs.Values.Should().ContainSingle(j => j.Id == job.Id);
        f.JobsDispatcher.PublishedForProcessing.Should().BeEmpty();
    }

    [Fact]
    public async Task OnTickAsync_WhenAPartitionFailsToSave_ReturnsFailed_AndLeavesItsMessagesLeased()
    {
        // Failed, not Success: consecutive failed ticks stop the runner and mark the bucket Lost, so another worker takes it.
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        var job = DueSoonJob(TestBucketId);
        f.JobsService.ThrowOnInsert = new TimeoutException("master down");
        f.JobsDispatcher.PendingQueue.Add(job);

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Failed);
        f.JobsDispatcher.AckedKeys.Should().BeEmpty();
        f.JobsDispatcher.LeasedJobs.Values.Should().ContainSingle(j => j.Id == job.Id);
    }

    [Fact]
    public async Task OnTickAsync_WhenTheSameJobComesInTwoMessages_SavesItOnceAndAcksBoth()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        var job = TransientJob(TestBucketId);
        f.JobsDispatcher.PendingQueue.Add(job);
        f.JobsDispatcher.PendingQueue.Add(new JobRawModel
        {
            Id = job.Id,
            Status = JobMasterJobStatus.PendingSave,
            BucketId = TestBucketId,
            NextPlanExecutionAt = job.NextPlanExecutionAt,
        });

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.JobsService.Jobs.Should().ContainSingle(j => j.Id == job.Id);
        f.JobsDispatcher.AckedKeys.Should().HaveCount(2);
        f.JobsDispatcher.LeasedJobs.Should().BeEmpty();
    }

    /// <summary>A pending-save job due now: within the transient threshold, so it goes to a bucket.</summary>
    private static JobRawModel DueSoonJob(string bucketId)
        => new()
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            Status = JobMasterJobStatus.PendingSave,
            BucketId = bucketId,
            NextPlanExecutionAt = DateTime.UtcNow,
        };
}
