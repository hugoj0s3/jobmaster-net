using Castle.Core.Logging;
using FluentAssertions;
using Moq;
using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Services.Master;

namespace JobMaster.UnitTests.Services.Master;

public class MasterJobsServiceTests
{
    [Fact]
    public async Task UpsertAsync_WhenEntityDoesNotExist_ShouldAdd_AndNotUpdate()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);

        var id = Guid.NewGuid();
        var raw = new JobRawModel(clusterId)
        {
            Id = id,
            JobDefinitionId = "job-def",
            ScheduledAt = DateTime.UtcNow,
            OriginalScheduledAt = DateTime.UtcNow,
            Priority = JobMasterPriority.High,
            Status = JobMasterJobStatus.HeldOnMaster,
            Timeout = TimeSpan.FromSeconds(1),
            MaxNumberOfRetries = 0,
            CreatedAt = DateTime.UtcNow,
        };

        repo.Setup(x => x.GetAsync(id)).ReturnsAsync((JobRawModel?)null);
        repo.Setup(x => x.AddAsync(raw)).Returns(Task.CompletedTask);

        var sut = new MasterJobsService(clusterConfig, repo.Object, new Mock<IJobMasterLogger>().Object, new FakeRuntime(true));

        await sut.UpsertAsync(raw);

        repo.Verify(x => x.UpdateAsync(It.IsAny<JobRawModel>()), Times.Never);
        repo.VerifyAll();
    }

    [Fact]
    public async Task UpsertAsync_WhenEntityExists_ShouldUpdate_AndNotAdd()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);

        var id = Guid.NewGuid();
        var raw = new JobRawModel(clusterId)
        {
            Id = id,
            JobDefinitionId = "job-def",
            ScheduledAt = DateTime.UtcNow,
            OriginalScheduledAt = DateTime.UtcNow,
            Priority = JobMasterPriority.High,
            Status = JobMasterJobStatus.HeldOnMaster,
            Timeout = TimeSpan.FromSeconds(1),
            MaxNumberOfRetries = 0,
            CreatedAt = DateTime.UtcNow,
        };

        repo.Setup(x => x.GetAsync(id)).ReturnsAsync(raw);
        repo.Setup(x => x.UpdateAsync(raw)).Returns(Task.CompletedTask);

        var sut = new MasterJobsService(clusterConfig, repo.Object, new Mock<IJobMasterLogger>().Object, new FakeRuntime(true));

        await sut.UpsertAsync(raw);

        repo.Verify(x => x.AddAsync(It.IsAny<JobRawModel>()), Times.Never);
        repo.VerifyAll();
    }

    [Fact]
    public void Upsert_WhenEntityDoesNotExist_ShouldAdd_AndNotUpdate()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);

        var id = Guid.NewGuid();
        var raw = new JobRawModel(clusterId)
        {
            Id = id,
            JobDefinitionId = "job-def",
            ScheduledAt = DateTime.UtcNow,
            OriginalScheduledAt = DateTime.UtcNow,
            Priority = JobMasterPriority.High,
            Status = JobMasterJobStatus.HeldOnMaster,
            Timeout = TimeSpan.FromSeconds(1),
            MaxNumberOfRetries = 0,
            CreatedAt = DateTime.UtcNow,
        };

        repo.Setup(x => x.Get(id)).Returns((JobRawModel?)null);
        repo.Setup(x => x.Add(raw));

        var sut = new MasterJobsService(clusterConfig, repo.Object, new Mock<IJobMasterLogger>().Object, new FakeRuntime(true));

        sut.Upsert(raw);

        repo.Verify(x => x.Update(It.IsAny<JobRawModel>()), Times.Never);
        repo.VerifyAll();
    }

    [Fact]
    public void BulkUpdatePartitionLockId_WhenIdsEmpty_ShouldReturnFalse_AndNotCallRepo()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);

        var sut = new MasterJobsService(clusterConfig, repo.Object, new Mock<IJobMasterLogger>().Object, new FakeRuntime(true));

        var result = sut.BulkUpdatePartitionLockId(new List<Guid>(), lockId: 1, expiresAt: DateTime.UtcNow);

        result.Should().BeFalse();
        repo.Verify(x => x.BulkUpdatePartitionLockId(It.IsAny<IList<Guid>>(), It.IsAny<int>(), It.IsAny<DateTime>()), Times.Never);
    }

    [Fact]
    public void BulkUpdateStatus_WhenIdsEmpty_ShouldDoNothing()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = new Mock<IMasterJobsRepository>(MockBehavior.Strict);

        var sut = new MasterJobsService(clusterConfig, repo.Object, new Mock<IJobMasterLogger>().Object, new FakeRuntime(true));

        sut.BulkUpdateStatus(new List<Guid>(), JobMasterJobStatus.Succeeded, agentConnectionId: null, agentWorkerId: null, bucketId: null);

        repo.Verify(x => x.BulkUpdateStatus(It.IsAny<IList<Guid>>(), It.IsAny<JobMasterJobStatus>(), It.IsAny<string?>(), It.IsAny<string?>(), It.IsAny<string?>(), It.IsAny<IList<JobMasterJobStatus>>()), Times.Never);
    }

    private static string NewClusterId() => $"c{Guid.NewGuid():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);
}
