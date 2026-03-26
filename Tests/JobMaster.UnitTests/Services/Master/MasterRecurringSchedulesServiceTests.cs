using FluentAssertions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using Moq;
using JobMaster.Sdk.Services.Master;

namespace JobMaster.UnitTests.Services.Master;

public class MasterRecurringSchedulesServiceTests
{
    [Fact]
    public async Task UpsertAsync_ShouldDelegateToRepository()
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

        repo.Setup(x => x.UpsertAsync(raw)).Returns(Task.CompletedTask);

        var sut = new MasterRecurringSchedulesService(locker.Object, clusterConfig, repo.Object, new FakeRuntime(true));

        await sut.UpsertAsync(raw);

        repo.Verify(x => x.UpsertAsync(raw), Times.Once);
        repo.VerifyAll();
    }

    [Fact]
    public void Upsert_ShouldDelegateToRepository()
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

        repo.Setup(x => x.Upsert(raw));

        var sut = new MasterRecurringSchedulesService(locker.Object, clusterConfig, repo.Object, new FakeRuntime(true));

        sut.Upsert(raw);

        repo.Verify(x => x.Upsert(raw), Times.Once);
        repo.VerifyAll();
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
