using Castle.Core.Logging;
using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using Moq;
using JobMaster.Sdk.Services.Master;

namespace JobMaster.UnitTests.Services.Master;

public class MasterJobsServiceTests
{
    [Fact]
    public async Task UpsertAsync_ShouldDelegateToRepository()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);

        var id = Guid.NewGuid();
        var raw = new JobRawModel(clusterId)
        {
            Id = id,
            JobDefinitionId = "job-def",
            NextPlanExecutionAt = DateTime.UtcNow,
            ScheduledAt = DateTime.UtcNow,
            Priority = JobMasterPriority.High,
            Status = JobMasterJobStatus.OnMaster,
            Timeout = TimeSpan.FromSeconds(1),
            MaxNumberOfRetries = 0,
            CreatedAt = DateTime.UtcNow,
        };

        repo.Setup(x => x.UpsertAsync(raw)).Returns(Task.CompletedTask);

        var sut = new MasterJobsService(clusterConfig, repo.Object, new Mock<IJobMasterLogger>().Object, new FakeRuntime(true), new Mock<IKnownExceptionIdentifier>().Object);

        await sut.UpsertAsync(raw);

        repo.Verify(x => x.UpsertAsync(raw), Times.Once);
        repo.VerifyAll();
    }

    [Fact]
    public void Upsert_ShouldDelegateToRepository()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);

        var id = Guid.NewGuid();
        var raw = new JobRawModel(clusterId)
        {
            Id = id,
            JobDefinitionId = "job-def",
            NextPlanExecutionAt = DateTime.UtcNow,
            ScheduledAt = DateTime.UtcNow,
            Priority = JobMasterPriority.High,
            Status = JobMasterJobStatus.OnMaster,
            Timeout = TimeSpan.FromSeconds(1),
            MaxNumberOfRetries = 0,
            CreatedAt = DateTime.UtcNow,
        };

        repo.Setup(x => x.Upsert(raw));

        var sut = new MasterJobsService(clusterConfig, repo.Object, new Mock<IJobMasterLogger>().Object, new FakeRuntime(true), new Mock<IKnownExceptionIdentifier>().Object);

        sut.Upsert(raw);

        repo.Verify(x => x.Upsert(raw), Times.Once);
        repo.VerifyAll();
    }

    [Fact]
    public void BulkUpdateStatus_WhenIdsEmpty_ShouldDoNothing()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);

        var sut = new MasterJobsService(clusterConfig, repo.Object, new Mock<IJobMasterLogger>().Object, new FakeRuntime(true), new Mock<IKnownExceptionIdentifier>().Object);

        sut.BulkUpdateStatus(new List<Guid>(), JobMasterJobStatus.Succeeded, agentConnectionId: null, agentWorkerId: null, bucketId: null);

        repo.Verify(x => x.BulkUpdateStatus(It.IsAny<IList<Guid>>(), It.IsAny<JobMasterJobStatus>(), It.IsAny<string?>(), It.IsAny<string?>(), It.IsAny<string?>(), It.IsAny<IList<JobMasterJobStatus>>()), Times.Never);
    }

    private static string NewClusterId() => $"c{Guid.NewGuid():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);
}
