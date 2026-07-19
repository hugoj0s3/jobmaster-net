using System.Reflection;
using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;
using JobMaster.Sdk.Utils;
using Moq;

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
        f.ClusterConfig.Config = new ClusterConfigurationModel("test") { ClusterMode = ClusterMode.Migrating };

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

    // ── Fallback bucket ───────────────────────────────────────────────────────

    [Fact]
    public async Task HandleJobFallbackAssignmentAsync_WhenThresholdElapsed_CreatesFallbackBucketOnReservedConnectionAndDispatches()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ActiveClusterConfig();

        // Keep the fallback PollingJobsExecutionRunner's background loop inert.
        var engine = new Mock<IJobsExecutionEngine>(MockBehavior.Loose);
        engine.Setup(x => x.PulseAsync()).Returns(Task.CompletedTask);
        engine.Setup(x => x.CountOnBoardingAvailability()).Returns(0);
        f.Worker.Setup(x => x.GetOrCreateEngine(It.IsAny<JobMasterPriority>(), It.IsAny<string>())).Returns(engine.Object);

        var runner = new AssignJobsToBucketsRunner(f.Worker.Object);
        var job = OnMasterJob();

        // Seed the "first failure seen" clock so the fallback threshold is already elapsed —
        // mirrors what OnTickAsync would build up over repeated ticks without a real bucket.
        var firstFailureField = typeof(AssignJobsToBucketsRunner)
            .GetField("bucketAssignFirstFailure", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var bucketAssignFirstFailure = (Dictionary<string, DateTime>)firstFailureField.GetValue(runner)!;
        bucketAssignFirstFailure[$"{f.ClusterId}_{job.WorkerLane}_{job.Priority}"] = DateTime.UtcNow.AddMinutes(-10);

        var handleFallbackMethod = typeof(AssignJobsToBucketsRunner)
            .GetMethod("HandleJobFallbackAssignmentAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

        try
        {
            await (Task)handleFallbackMethod.Invoke(runner, new object[] { job })!;

            var fallbackBucket = f.Buckets.Buckets.Should().ContainSingle(b => b.BucketType == BucketType.Fallback)
                .Subject;
            fallbackBucket.AgentConnectionId.Name.Should().Be(JobMasterConstants.MasterFallbackAgentConnName);
            fallbackBucket.AgentConnectionId.ClusterId.Should().Be(f.ClusterId);
            fallbackBucket.Priority.Should().Be(JobMasterPriority.Critical);

            f.WorkerClusterOps.Verify(
                x => x.DispatchJobToBucketAsync(
                    f.Worker.Object,
                    It.Is<JobRawModel>(j => j.Id == job.Id),
                    It.Is<BucketModel>(b => b.Id == fallbackBucket.Id)),
                Times.Once);
        }
        finally
        {
            // Stop the internally-spawned fallback runner so it doesn't keep polling in the background.
            var fallBackRunnerField = typeof(AssignJobsToBucketsRunner)
                .GetField("fallBackRunner", BindingFlags.NonPublic | BindingFlags.Instance)!;
            if (fallBackRunnerField.GetValue(runner) is IJobMasterRunner fallBackRunner)
            {
                await fallBackRunner.StopAsync();
            }
        }
    }

    [Fact]
    public async Task HandleJobFallbackAssignmentAsync_WhenCriticalIsDisabled_FallsBackToHighPriority()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ActiveClusterConfig();
        f.Worker.Object.ClusterConnConfig.SetDisabledPriorities(new HashSet<JobMasterPriority> { JobMasterPriority.Critical });

        var engine = new Mock<IJobsExecutionEngine>(MockBehavior.Loose);
        engine.Setup(x => x.PulseAsync()).Returns(Task.CompletedTask);
        engine.Setup(x => x.CountOnBoardingAvailability()).Returns(0);
        f.Worker.Setup(x => x.GetOrCreateEngine(It.IsAny<JobMasterPriority>(), It.IsAny<string>())).Returns(engine.Object);

        var runner = new AssignJobsToBucketsRunner(f.Worker.Object);
        var job = OnMasterJob();

        var firstFailureField = typeof(AssignJobsToBucketsRunner)
            .GetField("bucketAssignFirstFailure", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var bucketAssignFirstFailure = (Dictionary<string, DateTime>)firstFailureField.GetValue(runner)!;
        bucketAssignFirstFailure[$"{f.ClusterId}_{job.WorkerLane}_{job.Priority}"] = DateTime.UtcNow.AddMinutes(-10);

        var handleFallbackMethod = typeof(AssignJobsToBucketsRunner)
            .GetMethod("HandleJobFallbackAssignmentAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

        try
        {
            await (Task)handleFallbackMethod.Invoke(runner, new object[] { job })!;

            f.Buckets.Buckets.Should().ContainSingle(b => b.BucketType == BucketType.Fallback)
                .Subject.Priority.Should().Be(JobMasterPriority.High);
        }
        finally
        {
            var fallBackRunnerField = typeof(AssignJobsToBucketsRunner)
                .GetField("fallBackRunner", BindingFlags.NonPublic | BindingFlags.Instance)!;
            if (fallBackRunnerField.GetValue(runner) is IJobMasterRunner fallBackRunner)
            {
                await fallBackRunner.StopAsync();
            }
        }
    }

    [Fact]
    public async Task HandleJobFallbackAssignmentAsync_WhenCriticalAndHighAreDisabled_FallsBackToMediumPriority()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ActiveClusterConfig();
        f.Worker.Object.ClusterConnConfig.SetDisabledPriorities(new HashSet<JobMasterPriority>
        {
            JobMasterPriority.Critical,
            JobMasterPriority.High,
        });

        var engine = new Mock<IJobsExecutionEngine>(MockBehavior.Loose);
        engine.Setup(x => x.PulseAsync()).Returns(Task.CompletedTask);
        engine.Setup(x => x.CountOnBoardingAvailability()).Returns(0);
        f.Worker.Setup(x => x.GetOrCreateEngine(It.IsAny<JobMasterPriority>(), It.IsAny<string>())).Returns(engine.Object);

        var runner = new AssignJobsToBucketsRunner(f.Worker.Object);
        var job = OnMasterJob();

        var firstFailureField = typeof(AssignJobsToBucketsRunner)
            .GetField("bucketAssignFirstFailure", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var bucketAssignFirstFailure = (Dictionary<string, DateTime>)firstFailureField.GetValue(runner)!;
        bucketAssignFirstFailure[$"{f.ClusterId}_{job.WorkerLane}_{job.Priority}"] = DateTime.UtcNow.AddMinutes(-10);

        var handleFallbackMethod = typeof(AssignJobsToBucketsRunner)
            .GetMethod("HandleJobFallbackAssignmentAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

        try
        {
            await (Task)handleFallbackMethod.Invoke(runner, new object[] { job })!;

            f.Buckets.Buckets.Should().ContainSingle(b => b.BucketType == BucketType.Fallback)
                .Subject.Priority.Should().Be(JobMasterPriority.Medium);
        }
        finally
        {
            var fallBackRunnerField = typeof(AssignJobsToBucketsRunner)
                .GetField("fallBackRunner", BindingFlags.NonPublic | BindingFlags.Instance)!;
            if (fallBackRunnerField.GetValue(runner) is IJobMasterRunner fallBackRunner)
            {
                await fallBackRunner.StopAsync();
            }
        }
    }

    [Fact]
    public async Task OnTickAsync_WhenFallbackBucketExists_HeartbeatsItsReservedConnection()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ActiveClusterConfig();

        var runner = new AssignJobsToBucketsRunner(f.Worker.Object);
        var fallbackConnectionId = new AgentConnectionId(f.ClusterId, JobMasterConstants.MasterFallbackAgentConnName);
        var fallbackBucketField = typeof(AssignJobsToBucketsRunner)
            .GetField("fallbackBucket", BindingFlags.NonPublic | BindingFlags.Instance)!;
        fallbackBucketField.SetValue(runner, new BucketModel(f.ClusterId)
        {
            Id = "fallback-bucket",
            AgentConnectionId = fallbackConnectionId,
            BucketType = BucketType.Fallback
        });

        await runner.OnTickAsync(CancellationToken.None);

        f.HeartbeatService.HeartbeatCalls.Should().Contain(
            (ResourceHeartbeatType.AgentConnection, fallbackConnectionId.IdValue));
    }

    [Fact]
    public async Task OnTickAsync_WhenNoFallbackBucketExists_DoesNotHeartbeatFallbackConnection()
    {
        var f = RunnerFixture.Create();
        f.ClusterConfig.Config = ActiveClusterConfig();

        var runner = new AssignJobsToBucketsRunner(f.Worker.Object);

        await runner.OnTickAsync(CancellationToken.None);

        f.HeartbeatService.HeartbeatCalls.Should().BeEmpty();
    }
}
