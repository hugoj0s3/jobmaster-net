using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using Moq;

namespace JobMaster.UnitTests.Background;

public class JobsExecutionEngineTests
{
    // ── TryOnBoardingJobAsync ─────────────────────────────────────────────────

    [Fact]
    public async Task TryOnBoardingJobAsync_WhenCapacityAvailable_ShouldReturnAccepted()
    {
        var f = JobsExecutionEngineFixture.Create(bufferSize: 5);
        var availabilityBefore = f.Engine.CountOnBoardingAvailability();
        var job = JobsExecutionEngineFixture.CreateInBucketJob();

        var result = await f.Engine.TryOnBoardingJobAsync(job);

        result.Should().Be(OnBoardingResult.Accepted);
        f.Engine.CountOnBoardingAvailability().Should().Be(availabilityBefore - 1);
    }

    [Fact]
    public async Task TryOnBoardingJobAsync_WhenCapacityFull_ShouldReturnBusy()
    {
        const int bufferSize = 3;
        var f = JobsExecutionEngineFixture.Create(bufferSize: bufferSize);

        for (var i = 0; i < bufferSize; i++)
            await f.Engine.TryOnBoardingJobAsync(JobsExecutionEngineFixture.CreateInBucketJob());

        var result = await f.Engine.TryOnBoardingJobAsync(JobsExecutionEngineFixture.CreateInBucketJob());

        result.Should().Be(OnBoardingResult.Busy);
    }

    [Fact]
    public async Task TryOnBoardingJobAsync_WhenCapacityFullButForced_ShouldReturnAccepted()
    {
        const int bufferSize = 3;
        var f = JobsExecutionEngineFixture.Create(bufferSize: bufferSize);

        for (var i = 0; i < bufferSize; i++)
            await f.Engine.TryOnBoardingJobAsync(JobsExecutionEngineFixture.CreateInBucketJob());

        var result = await f.Engine.TryOnBoardingJobAsync(
            JobsExecutionEngineFixture.CreateInBucketJob(), forceIfNoCapacity: true);

        result.Should().Be(OnBoardingResult.Accepted);
    }

    [Fact]
    public async Task TryOnBoardingJobAsync_WhenJobNotYetDue_ShouldReturnTooEarly()
    {
        var f = JobsExecutionEngineFixture.Create();
        var job = JobsExecutionEngineFixture.CreateInBucketJob(nextPlanExecutionAt: DateTime.UtcNow.AddHours(1));

        var result = await f.Engine.TryOnBoardingJobAsync(job);

        result.Should().Be(OnBoardingResult.TooEarly);
    }

    [Fact]
    public async Task TryOnBoardingJobAsync_WhenProcessDeadlineIsNull_ShouldReturnInvalid()
    {
        var f = JobsExecutionEngineFixture.Create();
        var job = new JobRawModel
        {
            Id = Guid.NewGuid(),
            Status = JobMasterJobStatus.InBucket,
            NextPlanExecutionAt = DateTime.UtcNow.AddMinutes(-1),
            ProcessDeadline = null,
        };

        var result = await f.Engine.TryOnBoardingJobAsync(job);

        result.Should().Be(OnBoardingResult.Invalid);
    }

    [Fact]
    public async Task TryOnBoardingJobAsync_WhenJobNotInBucketStatus_ShouldReturnInvalid()
    {
        var f = JobsExecutionEngineFixture.Create();
        var job = new JobRawModel
        {
            Id = Guid.NewGuid(),
            Status = JobMasterJobStatus.OnMaster,
            NextPlanExecutionAt = DateTime.UtcNow.AddMinutes(-1),
            ProcessDeadline = DateTime.UtcNow.AddMinutes(10),
        };

        var result = await f.Engine.TryOnBoardingJobAsync(job);

        result.Should().Be(OnBoardingResult.Invalid);
    }

    [Fact]
    public async Task TryOnBoardingJobAsync_WhenRecurringScheduleNotFound_ShouldFailJobAndReturnCancelled()
    {
        var f = JobsExecutionEngineFixture.Create();
        var sourceId = Guid.NewGuid();
        var job = JobsExecutionEngineFixture.CreateRecurringJob(sourceId);

        f.Schedules.Setup(x => x.GetAsync(sourceId)).ReturnsAsync((RecurringScheduleRawModel?)null);

        var result = await f.Engine.TryOnBoardingJobAsync(job);

        result.Should().Be(OnBoardingResult.Cancelled);
        job.Status.Should().Be(JobMasterJobStatus.Failed);
        f.SingleUpdateWatcher.Should().ContainSingle(j => j.Id == job.Id);
    }

    [Fact]
    public async Task TryOnBoardingJobAsync_WhenRecurringScheduleTerminated_ShouldCancelJobAndReturnCancelled()
    {
        var f = JobsExecutionEngineFixture.Create();
        var sourceId = Guid.NewGuid();
        var job = JobsExecutionEngineFixture.CreateRecurringJob(sourceId);

        var schedule = new RecurringScheduleRawModel
        {
            Id = sourceId,
            Status = RecurringScheduleStatus.Canceled,
            TerminatedAt = DateTime.UtcNow.AddHours(-2),
        };
        f.Schedules.Setup(x => x.GetAsync(sourceId)).ReturnsAsync(schedule);

        var result = await f.Engine.TryOnBoardingJobAsync(job);

        result.Should().Be(OnBoardingResult.Cancelled);
        job.Status.Should().Be(JobMasterJobStatus.Cancelled);
        f.SingleUpdateWatcher.Should().ContainSingle(j => j.Id == job.Id);
    }

    [Fact]
    public async Task TryOnBoardingJobAsync_WhenRecurringScheduleStaticIdle_ShouldMoveToMasterAndReturnMovedToMaster()
    {
        var f = JobsExecutionEngineFixture.Create();
        var sourceId = Guid.NewGuid();
        var job = JobsExecutionEngineFixture.CreateRecurringJob(sourceId);

        var schedule = new RecurringScheduleRawModel
        {
            Id = sourceId,
            Status = RecurringScheduleStatus.Active,
            RecurringScheduleType = RecurringScheduleType.Static,
            StaticDefinitionLastEnsured = DateTime.UtcNow.AddHours(-1),
        };
        f.Schedules.Setup(x => x.GetAsync(sourceId)).ReturnsAsync(schedule);

        var result = await f.Engine.TryOnBoardingJobAsync(job);

        result.Should().Be(OnBoardingResult.MovedToMaster);
        job.Status.Should().Be(JobMasterJobStatus.OnMaster);
        f.SingleUpdateWatcher.Should().ContainSingle(j => j.Id == job.Id);
    }

    // ── PulseAsync / FlushToOnBoardingControlAsync ────────────────────────────

    [Fact]
    public async Task PulseAsync_WhenJobsStaged_ShouldBulkUpdateAsOnboarded()
    {
        var f = JobsExecutionEngineFixture.Create();
        // Future departure: accepted by TryOnBoarding (within 45s window) but won't be
        // pulled into the TaskQueue by EnqueueJobsAsync during this pulse.
        var job = JobsExecutionEngineFixture.CreateInBucketJob(nextPlanExecutionAt: DateTime.UtcNow.AddSeconds(30));
        await f.Engine.TryOnBoardingJobAsync(job);

        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.ActiveBucket(f.ClusterId, f.BucketId));

        await f.Engine.PulseAsync();

        f.BulkUpdateWatcher.Should().ContainSingle(j => j.Id == job.Id);
    }

    [Fact]
    public async Task PulseAsync_WhenNoJobsStaged_ShouldNotCallBulkUpdate()
    {
        var f = JobsExecutionEngineFixture.Create();
        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.ActiveBucket(f.ClusterId, f.BucketId));

        await f.Engine.PulseAsync();

        f.BulkUpdateWatcher.Should().BeEmpty();
    }

    [Fact]
    public async Task PulseAsync_WhenJobsExceedPartitionSize_ShouldBulkUpdateAllAcrossPartitions()
    {
        // 60 jobs → 2 partitions (50 + 10) inside FlushToOnBoardingControlAsync
        const int jobCount = 60;
        var f = JobsExecutionEngineFixture.Create(bufferSize: jobCount + 10);
        var jobs = JobsExecutionEngineFixture.CreateInBucketJobMany(
            nextPlanExecutionAt: DateTime.UtcNow.AddSeconds(30),
            count: jobCount);

        foreach (var job in jobs)
            await f.Engine.TryOnBoardingJobAsync(job);

        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.ActiveBucket(f.ClusterId, f.BucketId));

        await f.Engine.PulseAsync();

        f.BulkUpdateWatcher.Should().HaveCount(jobCount);
        f.BulkUpdateWatcher.Select(j => j.Id).Should()
            .BeEquivalentTo(jobs.Select(j => j.Id));
    }

    [Fact]
    public async Task PulseAsync_WhenBucketNotActive_ShouldNotEnqueueJobsToTaskQueue()
    {
        var f = JobsExecutionEngineFixture.Create();
        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(new BucketModel(f.ClusterId) { Id = f.BucketId, Status = BucketStatus.Lost });

        await f.Engine.PulseAsync();

        f.Engine.TaskQueueControl.CountWaiting().Should().Be(0);
    }

    // ── PulseAsync / Completing ───────────────────────────────────────────────

    [Fact]
    public async Task PulseAsync_WhenBucketCompleting_ShouldMovePendingJobsToMaster()
    {
        var f = JobsExecutionEngineFixture.Create(bufferSize: 10);

        // Push two onboarded jobs with a future departure — they sit in the
        // "pending, not yet ready" portion of OnBoardingControl.
        var job1 = JobsExecutionEngineFixture.CreateInBucketJob(nextPlanExecutionAt: DateTime.UtcNow.AddMinutes(5));
        var job2 = JobsExecutionEngineFixture.CreateInBucketJob(nextPlanExecutionAt: DateTime.UtcNow.AddMinutes(5));
        f.Engine.OnBoardingControl.Push(job1, job1.Id.ToString(), DateTime.UtcNow.AddMinutes(5));
        f.Engine.OnBoardingControl.Push(job2, job2.Id.ToString(), DateTime.UtcNow.AddMinutes(5));

        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.CompletingBucket(f.ClusterId, f.BucketId));

        await f.Engine.PulseAsync();

        // Both jobs must be upserted as HeldOnMaster — nothing should remain in OnBoardingControl.
        f.SingleUpdateWatcher.Should().HaveCount(2);
        f.SingleUpdateWatcher.Should().AllSatisfy(j => j.Status.Should().Be(JobMasterJobStatus.OnMaster));
        f.SingleUpdateWatcher.Select(j => j.Id).Should()
            .BeEquivalentTo(new[] { job1.Id, job2.Id });

        f.Engine.OnBoardingControl.CountAvailability().Should().Be(f.BufferSize);
    }

    [Fact]
    public async Task PulseAsync_WhenBucketCompletingAndJobExceedsDeadline_ShouldStillAttemptToMoveToMaster()
    {
        // ExceedProcessDeadline is unreliable on transport-layer data — we no longer skip
        // expired jobs during the Completing drain. Both jobs are moved to master and a
        // version conflict (caught by the caller) is the signal that another runner already
        // claimed an expired job.
        var f = JobsExecutionEngineFixture.Create(bufferSize: 10);

        var expiredJob = JobsExecutionEngineFixture.CreateInBucketJob(nextPlanExecutionAt: DateTime.UtcNow.AddMinutes(5));
        expiredJob.ProcessDeadline = DateTime.UtcNow.AddMinutes(-5);

        var validJob = JobsExecutionEngineFixture.CreateInBucketJob(nextPlanExecutionAt: DateTime.UtcNow.AddMinutes(5));

        f.Engine.OnBoardingControl.Push(expiredJob, expiredJob.Id.ToString(), DateTime.UtcNow.AddMinutes(5));
        f.Engine.OnBoardingControl.Push(validJob, validJob.Id.ToString(), DateTime.UtcNow.AddMinutes(5));

        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.CompletingBucket(f.ClusterId, f.BucketId));

        await f.Engine.PulseAsync();

        // Both jobs are upserted as OnMaster — expired job is no longer silently skipped.
        f.SingleUpdateWatcher.Should().HaveCount(2);
        f.SingleUpdateWatcher.Should().AllSatisfy(j => j.Status.Should().Be(JobMasterJobStatus.OnMaster));
        f.SingleUpdateWatcher.Select(j => j.Id).Should()
            .BeEquivalentTo(new[] { expiredJob.Id, validJob.Id });
    }

    [Fact]
    public async Task TryOnBoardingJobAsync_WhenBucketIsCompleting_ShouldMoveJobToMasterAndReturnMovedToMaster()
    {
        var f = JobsExecutionEngineFixture.Create();
        var job = JobsExecutionEngineFixture.CreateInBucketJob();

        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.CompletingBucket(f.ClusterId, f.BucketId));

        var result = await f.Engine.TryOnBoardingJobAsync(job);

        result.Should().Be(OnBoardingResult.MovedToMaster);
        job.Status.Should().Be(JobMasterJobStatus.OnMaster);
        f.SingleUpdateWatcher.Should().ContainSingle(j =>
            j.Id == job.Id && j.Status == JobMasterJobStatus.OnMaster);
    }

    [Fact]
    public async Task PulseAsync_WhenBucketCompletingAndEngineIsIdle_ShouldMarkBucketAsReadyToDrain()
    {
        // Engine is idle: no staged, onboarded, waiting, or running jobs.
        var f = JobsExecutionEngineFixture.Create();

        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.CompletingBucket(f.ClusterId, f.BucketId));

        await f.Engine.PulseAsync();

        f.Ops.Verify(x => x.MarkBucketAsReadyToDrainAsync(f.BucketId), Times.Once);
    }

    [Fact]
    public async Task PulseAsync_WhenBucketCompletingAndOnBoardingControlHasJobs_ShouldNotMarkReadyToDrainUntilDrained()
    {
        // Push a job into OnBoardingControl before the pulse. PullPendingJobsAsync will drain
        // it during the same pulse, so the engine becomes idle and ReadyToDrain is triggered.
        // A second pulse with nothing remaining should also trigger it.
        var f = JobsExecutionEngineFixture.Create(bufferSize: 10);

        var job = JobsExecutionEngineFixture.CreateInBucketJob(nextPlanExecutionAt: DateTime.UtcNow.AddMinutes(5));
        f.Engine.OnBoardingControl.Push(job, job.Id.ToString(), DateTime.UtcNow.AddMinutes(5));

        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.CompletingBucket(f.ClusterId, f.BucketId));

        await f.Engine.PulseAsync();

        // After drain the engine is idle — ReadyToDrain must be signalled.
        f.Engine.OnBoardingControl.CountItems().Should().Be(0);
        f.Ops.Verify(x => x.MarkBucketAsReadyToDrainAsync(f.BucketId), Times.Once);
    }

    // ── FlushToMasterAsync ────────────────────────────────────────────────────

    [Fact]
    public async Task FlushToMasterAsync_WhenJobsStaged_ShouldMarkAllOnMasterAndBulkUpdate()
    {
        var f = JobsExecutionEngineFixture.Create();
        var job = JobsExecutionEngineFixture.CreateInBucketJob();
        await f.Engine.TryOnBoardingJobAsync(job);

        await f.Engine.FlushToMasterAsync();

        job.Status.Should().Be(JobMasterJobStatus.OnMaster);
        f.Ops.Verify(
            x => x.BulkUpdateAsync(It.Is<BulkJobUpdateRequest>(r => r.JobIds.Contains(job.Id))),
            Times.Once);
    }

    [Fact]
    public async Task FlushToMasterAsync_WhenJobsExceedPartitionSize_ShouldCallBulkUpdateMultipleTimes()
    {
        // 60 jobs → 2 partitions (50 + 10) inside FlushToMasterAsync
        const int jobCount = 60;
        var f = JobsExecutionEngineFixture.Create(bufferSize: jobCount + 10);
        var jobs = JobsExecutionEngineFixture.CreateInBucketJobMany(count: jobCount);

        foreach (var job in jobs)
            await f.Engine.TryOnBoardingJobAsync(job);

        await f.Engine.FlushToMasterAsync();

        jobs.Should().AllSatisfy(j => j.Status.Should().Be(JobMasterJobStatus.OnMaster));
        f.Ops.Verify(x => x.BulkUpdateAsync(It.IsAny<BulkJobUpdateRequest>()), Times.Exactly(2));
    }

    [Fact]
    public async Task FlushToMasterAsync_WhenBulkUpdateThrows_ShouldNotRethrow()
    {
        var f = JobsExecutionEngineFixture.Create();
        await f.Engine.TryOnBoardingJobAsync(JobsExecutionEngineFixture.CreateInBucketJob());

        f.Jobs.Setup(x => x.BulkUpdateAsync(It.IsAny<BulkJobUpdateRequest>()))
            .ThrowsAsync(new Exception("db unavailable"));

        var act = async () => await f.Engine.FlushToMasterAsync();

        await act.Should().NotThrowAsync();
    }

    // ── Simulation ────────────────────────────────────────────────────────────

    [Fact]
    public async Task Simulate_SinglePulse_ShouldExecuteAllReadyJobs()
    {
        // High priority = 5 parallel slots, 250 ms priority delay per slot.
        // 5 jobs fit in one pulse and run concurrently — 1 s wait is ample.
        var f = JobsExecutionEngineFixture.Create(bufferSize: 10);
        f.ClusterCfg.Setup(x => x.Get()).Returns(JobsExecutionEngineFixture.ActiveClusterConfig(f.ClusterId));
        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.ActiveBucket(f.ClusterId, f.BucketId));

        var jobs = JobsExecutionEngineFixture.CreateInBucketJobMany(count: 5);
        foreach (var job in jobs)
            await f.Engine.TryOnBoardingJobAsync(job);

        await f.Engine.PulseAsync();

        await Task.Delay(TimeSpan.FromSeconds(1));

        f.BulkUpdateWatcher.Should().HaveCount(5);
        f.BulkUpdateWatcher.Should().AllSatisfy(j => j.Status.Should().Be(JobMasterJobStatus.Onboarded));

        // Each job produces 3 single-updates: Queued → Processing → Succeeded
        f.SingleUpdateWatcher.Should().HaveCount(15);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Queued).Should().HaveCount(5);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Processing).Should().HaveCount(5);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Succeeded).Should().HaveCount(5);

        f.Handler.ExecutionCount.Should().Be(5);
        f.Handler.ExecutedJobs.Select(j => j.JobDefinitionId)
            .Should().AllBe(FakeJobHandler.DefinitionId);

        AssertControlsEmpty(f);
    }

    [Fact]
    public async Task Simulate_MultiPulse_ShouldProcessJobsAsTheyBecomeReady()
    {
        // Batch A is past-due (executes on pulse 1).
        // Batch B departs 300 ms later (needs pulse 2 after the wait).
        var f = JobsExecutionEngineFixture.Create(bufferSize: 10);
        f.ClusterCfg.Setup(x => x.Get()).Returns(JobsExecutionEngineFixture.ActiveClusterConfig(f.ClusterId));
        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.ActiveBucket(f.ClusterId, f.BucketId));

        var batchA = JobsExecutionEngineFixture.CreateInBucketJobMany(count: 3);
        var batchB = JobsExecutionEngineFixture.CreateInBucketJobMany(
            nextPlanExecutionAt: DateTime.UtcNow.AddMilliseconds(300), count: 2);

        foreach (var job in batchA.Concat(batchB))
            await f.Engine.TryOnBoardingJobAsync(job);

        // Pulse 1 — flushes all 5 to OnBoardingControl; batch A is ready, batch B departs later.
        await f.Engine.PulseAsync();

        f.BulkUpdateWatcher.Should().HaveCount(5);

        await Task.Delay(TimeSpan.FromSeconds(1));

        f.Handler.ExecutionCount.Should().Be(3); // only batch A ran
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Queued).Should().HaveCount(3);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Processing).Should().HaveCount(3);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Succeeded).Should().HaveCount(3);

        // Wait past batch B departure time, then pulse again.
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        await f.Engine.PulseAsync();

        await Task.Delay(TimeSpan.FromSeconds(1));

        f.Handler.ExecutionCount.Should().Be(5); // all done
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Queued).Should().HaveCount(5);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Processing).Should().HaveCount(5);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Succeeded).Should().HaveCount(5);

        AssertControlsEmpty(f);
    }

    [Fact]
    public async Task Simulate_ManyJobs_ShouldProcessAllAcrossPartitionsAndSlots()
    {
        // 25 jobs, High priority (5 slots) → 5 concurrent waves of 5.
        // BulkUpdateAsync is called in 1 partition (25 < 50).
        const int jobCount = 25;
        var f = JobsExecutionEngineFixture.Create(bufferSize: jobCount + 5);
        f.ClusterCfg.Setup(x => x.Get()).Returns(JobsExecutionEngineFixture.ActiveClusterConfig(f.ClusterId));
        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.ActiveBucket(f.ClusterId, f.BucketId));

        var jobs = JobsExecutionEngineFixture.CreateInBucketJobMany(count: jobCount);
        foreach (var job in jobs)
            await f.Engine.TryOnBoardingJobAsync(job);

        await f.Engine.PulseAsync();

        // 25 jobs ÷ 5 slots = 5 waves × 250 ms each → ~1.25 s; 4 s gives plenty of margin.
        await Task.Delay(TimeSpan.FromSeconds(4));

        f.BulkUpdateWatcher.Should().HaveCount(jobCount);
        f.BulkUpdateWatcher.Should().AllSatisfy(j => j.Status.Should().Be(JobMasterJobStatus.Onboarded));

        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Queued).Should().HaveCount(jobCount);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Processing).Should().HaveCount(jobCount);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Succeeded).Should().HaveCount(jobCount);

        f.Handler.ExecutionCount.Should().Be(jobCount);

        AssertControlsEmpty(f);
    }

    [Fact]
    public async Task Simulate_WhenAllJobsFail_ShouldMarkAllAsFailed()
    {
        var f = JobsExecutionEngineFixture.Create(bufferSize: 10);
        f.ClusterCfg.Setup(x => x.Get()).Returns(JobsExecutionEngineFixture.ActiveClusterConfig(f.ClusterId));
        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.ActiveBucket(f.ClusterId, f.BucketId));

        f.Handler.ShouldFail = _ => true;

        var jobs = JobsExecutionEngineFixture.CreateInBucketJobMany(count: 5);
        foreach (var job in jobs)
            await f.Engine.TryOnBoardingJobAsync(job);

        await f.Engine.PulseAsync();

        await Task.Delay(TimeSpan.FromSeconds(1));

        f.BulkUpdateWatcher.Should().HaveCount(5);
        f.BulkUpdateWatcher.Should().AllSatisfy(j => j.Status.Should().Be(JobMasterJobStatus.Onboarded));

        // Each job: Queued (PreEnqueuedAsync) → Processing (ExecuteJobAsync start) → Failed (HandleErrorAsync)
        f.SingleUpdateWatcher.Should().HaveCount(15);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Queued).Should().HaveCount(5);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Processing).Should().HaveCount(5);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Failed).Should().HaveCount(5);

        f.Handler.ExecutionCount.Should().Be(0);

        AssertControlsEmpty(f);
    }

    [Fact]
    public async Task Simulate_WhenSomeJobsFail_ShouldMarkOnlyFailedJobsFailed()
    {
        // 6 jobs: first 3 fail, last 3 succeed.
        var f = JobsExecutionEngineFixture.Create(bufferSize: 10);
        f.ClusterCfg.Setup(x => x.Get()).Returns(JobsExecutionEngineFixture.ActiveClusterConfig(f.ClusterId));
        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.ActiveBucket(f.ClusterId, f.BucketId));

        var failingJobs = JobsExecutionEngineFixture.CreateInBucketJobMany(count: 3);
        var succeedingJobs = JobsExecutionEngineFixture.CreateInBucketJobMany(count: 3);
        var failingIds = failingJobs.Select(j => j.Id).ToHashSet();

        f.Handler.ShouldFail = ctx => failingIds.Contains(ctx.Id);

        foreach (var job in failingJobs.Concat(succeedingJobs))
            await f.Engine.TryOnBoardingJobAsync(job);

        await f.Engine.PulseAsync();

        await Task.Delay(TimeSpan.FromSeconds(1));

        f.BulkUpdateWatcher.Should().HaveCount(6);

        f.SingleUpdateWatcher.Should().HaveCount(18); // 6 jobs × 3 updates each
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Queued).Should().HaveCount(6);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Processing).Should().HaveCount(6);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Failed).Should().HaveCount(3);
        f.SingleUpdateWatcher.Where(j => j.Status == JobMasterJobStatus.Succeeded).Should().HaveCount(3);

        f.Handler.ExecutionCount.Should().Be(3);

        AssertControlsEmpty(f);
    }

    // ── CheckDeadlineJobsAsync ────────────────────────────────────────────────

    [Fact]
    public async Task CheckDeadline_WhenUntrackedJobWithCapacityAvailable_ShouldMoveToMaster()
    {
        // Engine is empty (no staged, onboarded, or running jobs) so the job is untracked
        // and the bucket has full capacity → it must be moved to HeldOnMaster.
        var f = JobsExecutionEngineFixture.Create(bufferSize: 10);
        var deadlineJob = JobsExecutionEngineFixture.CreateDeadlineExceededJob();

        f.Jobs.Setup(x => x.QueryAsync(It.IsAny<JobQueryCriteria>()))
            .ReturnsAsync(new List<JobRawModel> { deadlineJob });

        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.ActiveBucket(f.ClusterId, f.BucketId));

        await f.Engine.PulseAsync();

        f.Ops.Verify(
            x => x.BulkUpdateAsync(It.Is<BulkJobUpdateRequest>(r => r.JobIds.Contains(deadlineJob.Id))),
            Times.Once);
    }

    [Fact]
    public async Task CheckDeadline_WhenJobIsTrackedByOnBoardingControl_ShouldSkipIt()
    {
        // The job is already inside OnBoardingControl — the safety guard must skip it
        // even though the deadline query returns it.
        var f = JobsExecutionEngineFixture.Create(bufferSize: 10);
        var trackedJob = JobsExecutionEngineFixture.CreateDeadlineExceededJob();

        f.Engine.OnBoardingControl.Push(
            trackedJob, trackedJob.Id.ToString(), trackedJob.GetSafeNextPlanExecutionAt());

        f.Jobs.Setup(x => x.QueryAsync(It.IsAny<JobQueryCriteria>()))
            .ReturnsAsync(new List<JobRawModel> { trackedJob });

        f.Buckets.Setup(x => x.Get(f.BucketId, It.IsAny<TimeSpan?>()))
            .Returns(JobsExecutionEngineFixture.ActiveBucket(f.ClusterId, f.BucketId));

        await f.Engine.PulseAsync();

        // Neither watcher should have received the job.
        f.BulkUpdateWatcher.Should().NotContain(j => j.Id == trackedJob.Id);
        f.Ops.Verify(
            x => x.BulkUpdateAsync(It.Is<BulkJobUpdateRequest>(r => r.JobIds.Contains(trackedJob.Id))),
            Times.Never);
    }


    // ── helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Drains completed task slots then asserts both controls are fully empty.
    /// Call after all background tasks have had time to finish.
    /// </summary>
    private static void AssertControlsEmpty(EngineFixture f)
    {
        f.Engine.TaskQueueControl.StartQueuedTasksIfHasSlotAvailable();

        f.Engine.TaskQueueControl.CountWaiting().Should().Be(0);
        f.Engine.TaskQueueControl.CountRunning().Should().Be(0);
        f.Engine.OnBoardingControl.CountAvailability().Should().Be(f.BufferSize);
    }
}
