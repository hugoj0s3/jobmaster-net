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
    public async Task OnTickAsync_WhenPendingJobsExist_ShouldProcessThemAndReturnSuccess()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));

        // Transient job: NextPlanExecutionAt > cutOffDate (now + 5 min) — takes the
        // held-on-master branch inside JobSavePendingOperation without needing a bucket selection.
        f.JobsDispatcher.PendingQueue.Add(TransientJob(TestBucketId));

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        result.Status.Should().Be(TicketResultStatus.Success);
        f.JobsDispatcher.PendingQueue.Should().BeEmpty(); // pulled from queue
        f.JobsDispatcher.RequeuedJobs.Should().BeEmpty(); // no failure re-queue
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
    }

    [Fact]
    public async Task OnTickAsync_WhenJobProcessingFails_ShouldRequeueJobAndReturnSkippedWithDelay()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        f.JobsDispatcher.PendingQueue.Add(TransientJob(TestBucketId));

        // Make the IWorkerClusterOperations ExecWithRetryAsync throw so that
        // JobSavePendingOperation.AddSavePendingJobAsync propagates the exception to the runner.
        f.WorkerClusterOps
            .Setup(x => x.ExecWithRetryAsync(
                It.IsAny<Func<IWorkerClusterOperations, Task>>(),
                It.IsAny<int>(),
                It.IsAny<int>()))
            .ThrowsAsync(new InvalidOperationException("simulated processing failure"));

        var runner = CreateRunner(f);
        var result = await runner.OnTickAsync(CancellationToken.None);

        // On failure the runner re-queues and returns a Skipped result with backoff.
        result.Status.Should().Be(TicketResultStatus.Skipped);
        result.Delay.Should().BeGreaterThan(TimeSpan.Zero);
        f.JobsDispatcher.RequeuedJobs.Should().HaveCount(1);
    }

    [Fact]
    public async Task OnTickAsync_WhenConsecutiveFailures_ShouldIncreaseDelay()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, TestBucketId));
        f.WorkerClusterOps
            .Setup(x => x.ExecWithRetryAsync(
                It.IsAny<Func<IWorkerClusterOperations, Task>>(),
                It.IsAny<int>(),
                It.IsAny<int>()))
            .ThrowsAsync(new InvalidOperationException("simulated failure"));

        var runner = CreateRunner(f);

        // First failure.
        f.JobsDispatcher.PendingQueue.Add(TransientJob(TestBucketId));
        var result1 = await runner.OnTickAsync(CancellationToken.None);

        // Second failure.
        f.JobsDispatcher.PendingQueue.Add(TransientJob(TestBucketId));
        var result2 = await runner.OnTickAsync(CancellationToken.None);

        // Delay should grow with each consecutive failure.
        result2.Delay.Should().BeGreaterThanOrEqualTo(result1.Delay);
    }
}
