using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

public class WorkerClusterOperationsTests
{
    private const string BucketId = "bucket-1";

    [Fact]
    public async Task BulkDispatchJobsToBucketAsync_WhenTheBucketEngineIsOnAnotherWorkerOfThisProcess_InjectsIntoIt()
    {
        var f = RunnerFixture.Create();
        var bucket = RunnerFixture.ActiveBucket(f.ClusterId, BucketId);
        var job = new JobRawModel(f.ClusterId)
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            ScheduledAt = DateTime.UtcNow,
            NextPlanExecutionAt = DateTime.UtcNow,
        };
        job.AssignToBucket(bucket);

        var engine = new Mock<IJobsExecutionEngine>();
        engine.Setup(x => x.HasOnBoardingAvailability()).Returns(true);
        engine.Setup(x => x.TryOnBoardingJobAsync(It.IsAny<JobRawModel>(), It.IsAny<bool>())).ReturnsAsync(OnBoardingResult.Accepted);
        var otherWorker = new Mock<IJobMasterBackgroundAgentWorker>();
        otherWorker.Setup(x => x.GetEngine(BucketId)).Returns(engine.Object);
        var runtime = new Mock<IJobMasterRuntime>();
        runtime.Setup(x => x.GetAllWorkers()).Returns(new[] { f.Worker.Object, otherWorker.Object });
        f.Worker.SetupGet(x => x.Runtime).Returns(runtime.Object); // f.Worker has no engine for the bucket

        var sut = new WorkerClusterOperations(
            f.Worker.Object.ClusterConnConfig, f.JobsDispatcher, f.JobsService, f.Locker, f.Buckets,
            f.AgentWorkers, f.RecurringSchedulesService, f.Logger.Object);

        await sut.BulkDispatchJobsToBucketAsync(f.Worker.Object, bucket, new List<JobRawModel> { job });

        engine.Verify(x => x.TryOnBoardingJobAsync(job, true), Times.Once);
        f.JobsDispatcher.PublishedForProcessing.Should().BeEmpty();
    }
}
