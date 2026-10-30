using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background.SavePendingJobs;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Background.Runners;

// The agent's save when the job is already on master (e.g. ConfirmOnMasterAsync or a receipt cancel inserted it
// first): treated as already handled, never dispatched.
public class JobSavePendingOperationTests
{
    private const string BucketId = "bucket-1";

    [Fact]
    public async Task AddSavePendingJobAsync_WhenDueNowAndAlreadyOnMaster_ReturnsAlreadyExists_WithoutDispatching()
    {
        var f = RunnerFixture.Create();
        f.Buckets.Buckets.Add(RunnerFixture.ActiveBucket(f.ClusterId, BucketId));
        var job = PendingSaveJob(f.ClusterId, DateTime.UtcNow);
        ThrowDuplicateOnAdd(f, job);

        // FakeAgentJobsDispatcherService.AddForProcessingAsync throws: a dispatch would fail the test.
        var result = await new JobSavePendingOperation(f.Worker.Object, BucketId)
            .AddSavePendingJobAsync(job, DateTime.UtcNow.AddMinutes(1));

        result.ResultCode.Should().Be(AddSavePendingResultCode.AlreadyExists);
        f.WorkerClusterOps.Verify(x => x.UpsertAsync(It.IsAny<JobRawModel>()), Times.Never);
    }

    [Fact]
    public async Task AddSavePendingJobAsync_WhenFutureAndAlreadyOnMaster_ReturnsAlreadyExists()
    {
        var f = RunnerFixture.Create();
        var job = PendingSaveJob(f.ClusterId, DateTime.UtcNow.AddHours(1));
        ThrowDuplicateOnAdd(f, job);

        var result = await new JobSavePendingOperation(f.Worker.Object, BucketId)
            .AddSavePendingJobAsync(job, DateTime.UtcNow.AddMinutes(1));

        result.ResultCode.Should().Be(AddSavePendingResultCode.AlreadyExists);
        f.WorkerClusterOps.Verify(x => x.UpsertAsync(It.IsAny<JobRawModel>()), Times.Never);
    }

    private static void ThrowDuplicateOnAdd(RunnerFixture f, JobRawModel job) =>
        f.WorkerClusterOps
            .Setup(x => x.AddAsync(It.IsAny<JobRawModel>()))
            .ThrowsAsync(new JobMasterDuplicationException(job.Id, "Job", new Exception("dup")));

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
