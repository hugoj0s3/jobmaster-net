using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.SavePendingJobs;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

/// <summary>
/// <see cref="JobSavePendingOperation"/>'s single-job path (the NATS save runner): where a job goes, and "saved"
/// meaning on master -- a job already on master (e.g. ConfirmOnMasterAsync or a receipt cancel inserted it first)
/// counts as saved and is never dispatched again.
/// </summary>
public class JobSavePendingOperationTests
{
    private const string BucketId = "bucket-1";
    private const string OtherBucketId = "bucket-2";

    [Fact]
    public async Task AssignAsync_WhenPastTheCutOff_HoldsItOnMaster()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, BucketId));
        var job = PendingSaveJob(f.ClusterId, DateTime.UtcNow.AddHours(1));

        var target = await Sut(f).AssignAsync(currentRunnerBucket: null, job, DateTime.UtcNow.AddMinutes(1));

        target.Should().BeNull();
        job.Status.Should().Be(JobMasterJobStatus.OnMaster);
    }

    [Fact]
    public async Task AssignAsync_WhenDueNowAndNoLocalEngine_AssignsItToTheSelectedBucket()
    {
        var f = RunnerFixture.Create();
        var current = RunnerFixture.ActiveBucket(f.ClusterId, BucketId);
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, OtherBucketId)); // what SelectBucketAsync picks
        f.Buckets.Buckets.Add(current);
        var job = PendingSaveJob(f.ClusterId, DateTime.UtcNow);

        var target = await Sut(f).AssignAsync(current, job, DateTime.UtcNow.AddMinutes(1));

        target!.Id.Should().Be(OtherBucketId);
        job.Status.Should().Be(JobMasterJobStatus.InBucket);
        job.BucketId.Should().Be(OtherBucketId);
    }

    [Fact]
    public async Task AssignAsync_WhenOnboardingAndTheLocalEngineHasRoom_PrefersTheCurrentBucket()
    {
        // Starts from memory: no processing message to write and pull.
        var f = RunnerFixture.Create();
        var current = RunnerFixture.ActiveBucket(f.ClusterId, BucketId);
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, OtherBucketId));
        f.Buckets.Buckets.Add(current);
        var engine = new Mock<IJobsExecutionEngine>();
        engine.Setup(x => x.HasOnBoardingAvailability()).Returns(true);
        f.Worker.Setup(x => x.GetEngine(BucketId)).Returns(engine.Object);
        var job = PendingSaveJob(f.ClusterId, DateTime.UtcNow);

        var target = await Sut(f).AssignAsync(current, job, DateTime.UtcNow.AddMinutes(1));

        target!.Id.Should().Be(BucketId);
        job.BucketId.Should().Be(BucketId);
    }

    [Fact]
    public async Task SaveAndDispatchAsync_WhenInsertedIntoABucket_DispatchesIt()
    {
        var f = RunnerFixture.Create();
        var bucket = RunnerFixture.ActiveBucket(f.ClusterId, BucketId);
        var job = PendingSaveJob(f.ClusterId, DateTime.UtcNow);
        job.AssignToBucket(bucket);

        (await Sut(f).SaveAndDispatchAsync(job, bucket)).Should().BeTrue();

        f.JobsService.Jobs.Should().ContainSingle(j => j.Id == job.Id);
        f.WorkerClusterOps.Verify(x => x.BulkDispatchJobsToBucketAsync(
            It.IsAny<IJobMasterBackgroundAgentWorker>(), bucket, It.Is<IList<JobRawModel>>(l => l.Single().Id == job.Id)), Times.Once);
    }

    [Fact]
    public async Task SaveAndDispatchAsync_WhenAlreadyOnMaster_IsSaved_WithoutDispatching()
    {
        var f = RunnerFixture.Create();
        var bucket = RunnerFixture.ActiveBucket(f.ClusterId, BucketId);
        var job = PendingSaveJob(f.ClusterId, DateTime.UtcNow);
        job.AssignToBucket(bucket);
        f.JobsService.Jobs.Add(new JobRawModel(f.ClusterId) { Id = job.Id, Status = JobMasterJobStatus.Cancelled });

        (await Sut(f).SaveAndDispatchAsync(job, bucket)).Should().BeTrue();

        f.WorkerClusterOps.Verify(x => x.BulkDispatchJobsToBucketAsync(
            It.IsAny<IJobMasterBackgroundAgentWorker>(), It.IsAny<BucketModel>(), It.IsAny<IList<JobRawModel>>()), Times.Never);
    }

    [Fact]
    public async Task SaveAndDispatchAsync_WhenNotConfirmedOnMaster_IsNotSaved()
    {
        var f = RunnerFixture.Create();
        var job = PendingSaveJob(f.ClusterId, DateTime.UtcNow.AddHours(1));
        job.MarkAsHeldOnMaster();
        f.JobsService.UnknownOnInsert.Add(job.Id);

        (await Sut(f).SaveAndDispatchAsync(job, targetBucket: null)).Should().BeFalse();
    }

    private static JobSavePendingOperation Sut(RunnerFixture f) => new(f.Worker.Object, BucketId);

    private static JobRawModel PendingSaveJob(string clusterId, DateTime scheduledAt) => new(clusterId)
    {
        Id = JobMasterRandomUtil.NewGuid4(),
        JobDefinitionId = "def",
        TriggerSourceType = JobMasterTriggerSourceType.Once,
        Priority = JobMasterPriority.Medium,
        ScheduledAt = scheduledAt,
        NextPlanExecutionAt = scheduledAt,
        Status = JobMasterJobStatus.PendingSave,
        Timeout = TimeSpan.FromSeconds(5),
        MsgData = "{}",
        CreatedAt = DateTime.UtcNow,
    };
}
