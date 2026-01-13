using FluentAssertions;
using Moq;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Services.Master;

namespace JobMaster.UnitTests.Services.Master;

public class MasterRecurringSchedulesServiceTests
{
    [Fact]
    public async Task UpsertAsync_WhenEntityDoesNotExist_ShouldAdd_ThenUpdate()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Loose);
        var repo = new Mock<IMasterRecurringSchedulesRepository>(MockBehavior.Strict);

        var id = Guid.NewGuid();
        var raw = new RecurringScheduleRawModel(clusterId)
        {
            Id = id,
            JobDefinitionId = "job-def",
            Expression = "* * * * *",
            ExpressionTypeId = "cron",
            CreatedAt = DateTime.UtcNow,
        };

        repo.Setup(x => x.GetAsync(id)).ReturnsAsync((RecurringScheduleRawModel?)null);
        repo.Setup(x => x.AddAsync(raw)).Returns(Task.CompletedTask);
        repo.Setup(x => x.UpdateAsync(raw)).Returns(Task.CompletedTask);

        var sut = new MasterRecurringSchedulesService(locker.Object, clusterConfig, repo.Object, new FakeRuntime(true));

        await sut.UpsertAsync(raw);

        repo.VerifyAll();
    }

    [Fact]
    public async Task UpsertAsync_WhenEntityExists_ShouldOnlyUpdate()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Loose);
        var repo = new Mock<IMasterRecurringSchedulesRepository>(MockBehavior.Strict);

        var id = Guid.NewGuid();
        var raw = new RecurringScheduleRawModel(clusterId)
        {
            Id = id,
            JobDefinitionId = "job-def",
            Expression = "* * * * *",
            ExpressionTypeId = "cron",
            CreatedAt = DateTime.UtcNow,
        };

        repo.Setup(x => x.GetAsync(id)).ReturnsAsync(raw);
        repo.Setup(x => x.UpdateAsync(raw)).Returns(Task.CompletedTask);

        var sut = new MasterRecurringSchedulesService(locker.Object, clusterConfig, repo.Object, new FakeRuntime(true));

        await sut.UpsertAsync(raw);

        repo.Verify(x => x.AddAsync(It.IsAny<RecurringScheduleRawModel>()), Times.Never);
        repo.VerifyAll();
    }

    [Fact]
    public void Upsert_WhenEntityDoesNotExist_ShouldAdd_ThenUpdate()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Loose);
        var repo = new Mock<IMasterRecurringSchedulesRepository>(MockBehavior.Strict);

        var id = Guid.NewGuid();
        var raw = new RecurringScheduleRawModel(clusterId)
        {
            Id = id,
            JobDefinitionId = "job-def",
            Expression = "* * * * *",
            ExpressionTypeId = "cron",
            CreatedAt = DateTime.UtcNow,
        };

        repo.Setup(x => x.Get(id)).Returns((RecurringScheduleRawModel?)null);
        repo.Setup(x => x.Add(raw));
        repo.Setup(x => x.Update(raw));

        var sut = new MasterRecurringSchedulesService(locker.Object, clusterConfig, repo.Object, new FakeRuntime(true));

        sut.Upsert(raw);

        repo.VerifyAll();
    }

    [Fact]
    public void BulkUpdatePartitionLockId_WhenIdsEmpty_ShouldReturnFalse_AndNotCallRepo()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Loose);
        var repo = new Mock<IMasterRecurringSchedulesRepository>(MockBehavior.Strict);

        var sut = new MasterRecurringSchedulesService(locker.Object, clusterConfig, repo.Object, new FakeRuntime(true));

        var result = sut.BulkUpdatePartitionLockId(new List<Guid>(), lockId: 1, expiresAt: DateTime.UtcNow);

        result.Should().BeFalse();
        repo.Verify(x => x.BulkUpdatePartitionLockId(It.IsAny<IList<Guid>>(), It.IsAny<int>(), It.IsAny<DateTime>()), Times.Never);
    }

    [Fact]
    public void BulkUpdateStaticDefinitionLastEnsured_WhenIdsEmpty_ShouldDoNothing()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Loose);
        var repo = new Mock<IMasterRecurringSchedulesRepository>(MockBehavior.Strict);

        var sut = new MasterRecurringSchedulesService(locker.Object, clusterConfig, repo.Object, new FakeRuntime(true));

        sut.BulkUpdateStaticDefinitionLastEnsured(new List<string>(), DateTime.UtcNow);

        repo.Verify(x => x.BulkUpdateStaticDefinitionLastEnsuredByStaticIds(It.IsAny<IList<string>>(), It.IsAny<DateTime>()), Times.Never);
    }

    private static string NewClusterId() => $"c{Guid.NewGuid():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);
}
