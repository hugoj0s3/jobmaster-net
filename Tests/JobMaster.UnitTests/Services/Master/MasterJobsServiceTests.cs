using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Services.Master;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Services.Master;

public class MasterJobsServiceTests
{
    // ── ValidateJobExecutionOutcome: only Succeeded/Failed status are outcome-restricted ─────────────
    // A Failed execution outcome is valid alongside any status except Succeeded -- e.g. TryRetry()
    // (JobRawModel.cs) sets Status back to OnMaster, not Failed, when retries remain, so recording
    // that attempt's Failed execution alongside OnMaster status must be accepted. The only two
    // invariants actually enforced: Succeeded status requires a Succeeded outcome, and Failed status
    // requires a Failed outcome.

    [Fact]
    public async Task UpdateAsync_WhenJobIsOnMasterWithFailedExecution_ShouldNotThrow()
    {
        var clusterId = NewClusterId();
        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);
        var job = NewJob(clusterId, JobMasterJobStatus.OnMaster);
        var execution = NewFailedExecution(clusterId);

        repo.Setup(x => x.UpdateAsync(job, execution)).Returns(Task.CompletedTask);

        var sut = CreateSut(clusterId, repo.Object);

        var act = () => sut.UpdateAsync(job, execution);

        await act.Should().NotThrowAsync();
        repo.Verify(x => x.UpdateAsync(job, execution), Times.Once);
    }

    [Fact]
    public void Update_WhenJobIsOnMasterWithFailedExecution_ShouldNotThrow()
    {
        var clusterId = NewClusterId();
        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);
        var job = NewJob(clusterId, JobMasterJobStatus.OnMaster);
        var execution = NewFailedExecution(clusterId);

        repo.Setup(x => x.Update(job, execution));

        var sut = CreateSut(clusterId, repo.Object);

        var act = () => sut.Update(job, execution);

        act.Should().NotThrow();
        repo.Verify(x => x.Update(job, execution), Times.Once);
    }

    [Fact]
    public async Task UpdateAsync_WhenJobIsInBucketWithFailedExecution_ShouldNotThrow()
    {
        var clusterId = NewClusterId();
        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);
        var job = NewJob(clusterId, JobMasterJobStatus.InBucket);
        var execution = NewFailedExecution(clusterId);

        repo.Setup(x => x.UpdateAsync(job, execution)).Returns(Task.CompletedTask);

        var sut = CreateSut(clusterId, repo.Object);

        var act = () => sut.UpdateAsync(job, execution);

        await act.Should().NotThrowAsync();
        repo.Verify(x => x.UpdateAsync(job, execution), Times.Once);
    }

    [Fact]
    public async Task UpdateAsync_WhenJobSucceededWithFailedExecution_ShouldThrowArgumentException()
    {
        var clusterId = NewClusterId();
        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);
        var job = NewJob(clusterId, JobMasterJobStatus.Succeeded);
        var execution = NewFailedExecution(clusterId);

        var sut = CreateSut(clusterId, repo.Object);

        var act = () => sut.UpdateAsync(job, execution);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ── AcquireAndFetchAsync: deadlock handling ─────────────────────────────────────────────────────

    [Fact]
    public async Task AcquireAndFetchAsync_WhenRepositoryThrowsDeadlock_ShouldReturnEmptyList()
    {
        var clusterId = NewClusterId();
        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);
        var deadlockException = new InvalidOperationException("simulated deadlock");

        repo.Setup(x => x.AcquireAndFetchAsync(It.IsAny<JobQueryCriteria>(), It.IsAny<Guid>(), It.IsAny<DateTime>()))
            .ThrowsAsync(deadlockException);

        var exceptionIdentifier = new Mock<IKnownExceptionIdentifier>(MockBehavior.Strict);
        exceptionIdentifier.Setup(x => x.Identify(deadlockException)).Returns(JobMasterKnownExceptionId.Deadlock);

        var sut = CreateSut(clusterId, repo.Object, exceptionIdentifier.Object);

        var result = await sut.AcquireAndFetchAsync(new JobQueryCriteria(), DateTime.UtcNow);

        result.Should().BeEmpty();
    }

    [Fact]
    public async Task AcquireAndFetchAsync_WhenRepositoryThrowsNonDeadlockException_ShouldPropagate()
    {
        var clusterId = NewClusterId();
        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);
        var otherException = new InvalidOperationException("not a deadlock");

        repo.Setup(x => x.AcquireAndFetchAsync(It.IsAny<JobQueryCriteria>(), It.IsAny<Guid>(), It.IsAny<DateTime>()))
            .ThrowsAsync(otherException);

        var exceptionIdentifier = new Mock<IKnownExceptionIdentifier>(MockBehavior.Strict);
        exceptionIdentifier.Setup(x => x.Identify(otherException)).Returns((JobMasterKnownExceptionId?)null);

        var sut = CreateSut(clusterId, repo.Object, exceptionIdentifier.Object);

        var act = () => sut.AcquireAndFetchAsync(new JobQueryCriteria(), DateTime.UtcNow);

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────────

    private static string NewClusterId() => $"c{JobMasterRandomUtil.NewGuid4():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);

    private static MasterJobsService CreateSut(string clusterId, IMasterJobsRepository repo, IKnownExceptionIdentifier? exceptionIdentifier = null)
        => new(CreateClusterConfig(clusterId), repo, new Mock<IJobMasterLogger>().Object, exceptionIdentifier ?? new Mock<IKnownExceptionIdentifier>().Object);

    private static JobRawModel NewJob(string clusterId, JobMasterJobStatus status) => new(clusterId)
    {
        Id = JobMasterRandomUtil.NewGuid7(),
        JobDefinitionId = "job-def",
        Status = status,
        MaxNumberOfRetries = 3,
    };

    private static JobExecution NewFailedExecution(string clusterId)
    {
        var execution = new JobExecution(clusterId) { JobId = JobMasterRandomUtil.NewGuid7() };
        execution.Fail("simulated failure");
        return execution;
    }
}
