using FluentAssertions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using Moq;
using JobMaster.Sdk.Services.Master;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Services.Master;

public class MasterRecurringSchedulesServiceTests
{
    [Fact]
    public async Task AddAsync_ShouldDelegateToRepository()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Loose);
        var repo = new Mock<IMasterRecurringSchedulesRepository>(MockBehavior.Strict);

        var id = JobMasterRandomUtil.NewGuid4();
        var raw = new RecurringScheduleRawModel(clusterId)
        {
            Id = id,
            JobDefinitionId = "job-def",
            Expression = "* * * * *",
            ExpressionTypeId = "cron",
            CreatedAt = DateTime.UtcNow,
        };

        repo.Setup(x => x.AddAsync(raw)).Returns(Task.CompletedTask);

        var sut = new MasterRecurringSchedulesService(locker.Object, clusterConfig, repo.Object, new FakeRuntime(true), new Mock<IJobMasterLogger>().Object, new Mock<IKnownExceptionIdentifier>().Object);

        await sut.AddAsync(raw);

        repo.Verify(x => x.AddAsync(raw), Times.Once);
        repo.VerifyAll();
    }

    [Fact]
    public void Add_ShouldDelegateToRepository()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Loose);
        var repo = new Mock<IMasterRecurringSchedulesRepository>(MockBehavior.Strict);

        var id = JobMasterRandomUtil.NewGuid4();
        var raw = new RecurringScheduleRawModel(clusterId)
        {
            Id = id,
            JobDefinitionId = "job-def",
            Expression = "* * * * *",
            ExpressionTypeId = "cron",
            CreatedAt = DateTime.UtcNow,
        };

        repo.Setup(x => x.Add(raw));

        var sut = new MasterRecurringSchedulesService(locker.Object, clusterConfig, repo.Object, new FakeRuntime(true), new Mock<IJobMasterLogger>().Object, new Mock<IKnownExceptionIdentifier>().Object);

        sut.Add(raw);

        repo.Verify(x => x.Add(raw), Times.Once);
        repo.VerifyAll();
    }

    [Fact]
    public void BulkUpdateStaticDefinitionLastEnsured_WhenIdsEmpty_ShouldDoNothing()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Loose);
        var repo = new Mock<IMasterRecurringSchedulesRepository>(MockBehavior.Strict);

        var sut = new MasterRecurringSchedulesService(locker.Object, clusterConfig, repo.Object, new FakeRuntime(true), new Mock<IJobMasterLogger>().Object, new Mock<IKnownExceptionIdentifier>().Object);

        sut.BulkUpdateStaticDefinitionLastEnsured(new List<string>(), DateTime.UtcNow);

        repo.Verify(x => x.BulkUpdateStaticDefinitionLastEnsuredByStaticIds(It.IsAny<IList<string>>(), It.IsAny<DateTime>()), Times.Never);
    }

    private static string NewClusterId() => $"c{JobMasterRandomUtil.NewGuid4():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);
}
