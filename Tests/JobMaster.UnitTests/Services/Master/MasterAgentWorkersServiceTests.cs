using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using Moq;
using JobMaster.Sdk.Services.Master;

namespace JobMaster.UnitTests.Services.Master;

public class MasterAgentWorkersServiceTests
{
    [Fact]
    public void RegisterWorker_WhenGeneratedWorkerIdIsInvalid_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Loose);
        var clusterCfg = new Mock<IMasterClusterConfigurationService>(MockBehavior.Loose);
        var sentinel = new Mock<IMasterChangesSentinelService>(MockBehavior.Loose);
        var heartbeat = new Mock<IMasterHeartbeatService>(MockBehavior.Loose);
        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Loose);

        var sut = new MasterAgentWorkersService(
            clusterConfig,
            cache.Object,
            clusterCfg.Object,
            sentinel.Object,
            heartbeat.Object,
            repo.Object);

        var act = () => sut.RegisterWorker("agent", "invalid name", workerLane: null, AgentWorkerMode.Full, 1);
        act.Should().Throw<ArgumentException>().WithParameterName("workerName");

        repo.Verify(x => x.Insert(It.IsAny<GenericRecordEntry>()), Times.Never);
    }

    [Fact]
    public void RegisterWorker_ShouldInsert_InvalidateCache_AndNotifySentinel()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Loose);
        var clusterCfg = new Mock<IMasterClusterConfigurationService>(MockBehavior.Loose);
        var sentinel = new Mock<IMasterChangesSentinelService>(MockBehavior.Strict);
        var heartbeat = new Mock<IMasterHeartbeatService>(MockBehavior.Loose);
        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Strict);

        var expectedSentinelKey = new JobMasterSentinelKeys(clusterId).AgentsAndWorkers();
        sentinel.Setup(x => x.NotifyChanges(expectedSentinelKey));

        var expectedCacheKey = new JobMasterInMemoryKeys(clusterId).AllAgentsWorkers();
        cache.Setup(x => x.Remove(expectedCacheKey));

        GenericRecordEntry? inserted = null;
        repo.Setup(x => x.Insert(It.IsAny<GenericRecordEntry>()))
            .Callback<GenericRecordEntry>(e => inserted = e);

        var sut = new MasterAgentWorkersService(
            clusterConfig,
            cache.Object,
            clusterCfg.Object,
            sentinel.Object,
            heartbeat.Object,
            repo.Object);

        var workerId = sut.RegisterWorker($"{clusterId}:agent", "worker", workerLane: "lane", AgentWorkerMode.Full, 1);

        workerId.Should().NotBeNullOrWhiteSpace();

        inserted.Should().NotBeNull();
        inserted!.GroupId.Should().Be(MasterGenericRecordGroupIds.AgentWorker);
        inserted.EntryId.Should().Be(workerId);

        cache.VerifyAll();
        sentinel.VerifyAll();
        repo.VerifyAll();
    }

    [Fact]
    public void DeleteWorker_WhenWorkerDoesNotExist_ShouldDoNothing()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Loose);
        var clusterCfg = new Mock<IMasterClusterConfigurationService>(MockBehavior.Loose);
        var sentinel = new Mock<IMasterChangesSentinelService>(MockBehavior.Loose);
        var heartbeat = new Mock<IMasterHeartbeatService>(MockBehavior.Loose);
        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Strict);

        repo.Setup(x => x.Query(MasterGenericRecordGroupIds.AgentWorker, null))
            .Returns(new List<GenericRecordEntry>());

        var sut = new MasterAgentWorkersService(
            clusterConfig,
            cache.Object,
            clusterCfg.Object,
            sentinel.Object,
            heartbeat.Object,
            repo.Object);

        sut.DeleteWorker("missing");

        repo.Verify(x => x.Delete(It.IsAny<string>(), It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public void DeleteWorker_WhenWorkerIsAlive_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Loose);
        var clusterCfg = new Mock<IMasterClusterConfigurationService>(MockBehavior.Loose);
        var sentinel = new Mock<IMasterChangesSentinelService>(MockBehavior.Loose);
        var heartbeat = new Mock<IMasterHeartbeatService>(MockBehavior.Strict);
        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Strict);

        var createdAt = DateTime.UtcNow.AddHours(-1);
        var workerId = "w1";

        var entry = GenericRecordEntry.Create(
            clusterId,
            MasterGenericRecordGroupIds.AgentWorker,
            workerId,
            new AgentWorkerRecordDto
            {
                ClusterId = clusterId,
                Id = workerId,
                AgentConnectionId = $"{clusterId}:agent",
                Name = "worker",
                CreatedAt = createdAt,
                Mode = AgentWorkerMode.Full,
                WorkerLane = null
            });

        repo.Setup(x => x.Query(MasterGenericRecordGroupIds.AgentWorker, null))
            .Returns(new List<GenericRecordEntry> { entry });

        heartbeat.Setup(x => x.GetLastHeartbeats(It.Is<IList<string>>(ids => ids.Count == 1 && ids[0] == workerId)))
            .Returns(new Dictionary<string, DateTime?> { [workerId] = DateTime.UtcNow });

        var sut = new MasterAgentWorkersService(
            clusterConfig,
            cache.Object,
            clusterCfg.Object,
            sentinel.Object,
            heartbeat.Object,
            repo.Object);

        var act = () => sut.DeleteWorker(workerId);
        act.Should().Throw<InvalidOperationException>();

        repo.Verify(x => x.Delete(It.IsAny<string>(), It.IsAny<string>()), Times.Never);
        heartbeat.VerifyAll();
    }

    private sealed class AgentWorkerRecordDto
    {
        public string ClusterId { get; set; } = string.Empty;
        public string Id { get; set; } = string.Empty;
        public string AgentConnectionId { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public AgentWorkerMode Mode { get; set; }
        public string? WorkerLane { get; set; }
    }

    private static string NewClusterId() => $"c{Guid.NewGuid():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);
}
