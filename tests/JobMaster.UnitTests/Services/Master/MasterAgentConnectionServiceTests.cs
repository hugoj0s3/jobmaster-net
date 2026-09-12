using FluentAssertions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using Moq;
using JobMaster.Sdk.Services.Master;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Services.Master;

/// <summary>
/// Unit tests for <see cref="MasterAgentConnectionService"/>.
/// Covers: <c>ProtectConnectionChanges</c> being correctly persisted (and toggleable) on save,
/// resolving <c>LastHeartbeatAt</c> as whichever of the heartbeat log or <c>FingerprintCreatedAt</c>
/// is more recent (so a stale heartbeat log entry left over from a deleted-then-recreated connection
/// doesn't make it look older than it is), the per-connection distributed lock being
/// acquired/released around save and delete, and <c>SafeDeleteConnectionAsync</c> refusing to
/// delete when the lock is unavailable or the
/// connection still owns buckets.
/// </summary>
public class MasterAgentConnectionServiceTests
{
    private const string LockToken = "lock-token";

    [Fact]
    public async Task SaveConnectionAsync_WhenNewConnection_ShouldPersistProtectConnectionChanges()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = NewRepoMock();
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig, tryLockReturns: LockToken);
        var heartbeat = new Mock<IMasterHeartbeatService>(MockBehavior.Loose);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Loose);
        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Loose);
        var knownEx = new Mock<IKnownExceptionIdentifier>(MockBehavior.Loose);

        repo.Setup(x => x.GetAsync(MasterGenericRecordGroupIds.AgentConnection, It.IsAny<string>(), false))
            .ReturnsAsync((GenericRecordEntry?)null);

        GenericRecordEntry? upserted = null;
        repo.Setup(x => x.UpsertAsync(It.IsAny<GenericRecordEntry>()))
            .Callback<GenericRecordEntry>(e => upserted = e)
            .Returns(Task.CompletedTask);

        var sut = new MasterAgentConnectionService(
            clusterConfig, repo.Object, cache.Object, sentinel.Object, knownEx.Object,
            heartbeat.Object, buckets.Object, locker.Object);

        var connId = new AgentConnectionId(clusterId, "agent");
        var model = await sut.SaveConnectionAsync(connId, "repo", "fp1", protectChanges: true);

        model.ProtectConnectionChanges.Should().BeTrue();
        upserted.Should().NotBeNull();
        upserted!.ToObject<AgentConnectionRecordDto>()!.ProtectConnectionChanges.Should().BeTrue();

        locker.Verify(x => x.TryLock(It.IsAny<string>(), It.IsAny<TimeSpan>()), Times.Once);
        locker.Verify(x => x.ReleaseLock(It.IsAny<string>(), LockToken), Times.Once);
    }

    [Fact]
    public async Task SaveConnectionAsync_WhenExistingConnectionTogglesProtectionOff_ShouldUpdateIt()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = NewRepoMock();
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig, tryLockReturns: LockToken);
        var heartbeat = new Mock<IMasterHeartbeatService>(MockBehavior.Loose);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Loose);
        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Loose);
        var knownEx = new Mock<IKnownExceptionIdentifier>(MockBehavior.Loose);

        var connId = new AgentConnectionId(clusterId, "agent");
        var existing = GenericRecordEntry.Create(clusterId, MasterGenericRecordGroupIds.AgentConnection, connId.IdValue,
            new AgentConnectionRecordDto
            {
                ClusterId = clusterId,
                Id = connId.IdValue,
                Fingerprint = "fp1",
                CreatedAt = DateTime.UtcNow.AddDays(-1),
                FingerprintCreatedAt = DateTime.UtcNow.AddDays(-1),
                RepositoryTypeId = "repo",
                ProtectConnectionChanges = true,
            });

        repo.Setup(x => x.GetAsync(MasterGenericRecordGroupIds.AgentConnection, connId.IdValue, false))
            .ReturnsAsync(existing);
        repo.Setup(x => x.UpsertAsync(It.IsAny<GenericRecordEntry>())).Returns(Task.CompletedTask);

        var sut = new MasterAgentConnectionService(
            clusterConfig, repo.Object, cache.Object, sentinel.Object, knownEx.Object,
            heartbeat.Object, buckets.Object, locker.Object);

        var model = await sut.SaveConnectionAsync(connId, "repo", "fp1", protectChanges: false);

        model.ProtectConnectionChanges.Should().BeFalse();
    }

    [Fact]
    public async Task QueryAllAsync_WhenHeartbeatLogPredatesFingerprintCreatedAt_ShouldUseFingerprintCreatedAt()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = NewRepoMock();
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig, tryLockReturns: LockToken);
        var heartbeat = new Mock<IMasterHeartbeatService>(MockBehavior.Loose);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Loose);
        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Loose);
        var knownEx = new Mock<IKnownExceptionIdentifier>(MockBehavior.Loose);

        var connId = new AgentConnectionId(clusterId, "agent");
        // Connection was deleted and just recreated — fresh FingerprintCreatedAt, but the
        // heartbeat log entry from before the deletion was never cleared and is much older.
        var freshCreatedAt = DateTime.UtcNow;
        var staleHeartbeat = DateTime.UtcNow.AddDays(-10);

        var entry = GenericRecordEntry.Create(clusterId, MasterGenericRecordGroupIds.AgentConnection, connId.IdValue,
            new AgentConnectionRecordDto
            {
                ClusterId = clusterId,
                Id = connId.IdValue,
                Fingerprint = "fp1",
                CreatedAt = freshCreatedAt,
                FingerprintCreatedAt = freshCreatedAt,
                RepositoryTypeId = "repo",
            });

        repo.Setup(x => x.QueryAsync(MasterGenericRecordGroupIds.AgentConnection, null))
            .ReturnsAsync(new List<GenericRecordEntry> { entry });
        heartbeat.Setup(x => x.GetLastHeartbeats(ResourceHeartbeatType.AgentConnection, It.IsAny<IList<string>>()))
            .Returns(new Dictionary<string, DateTime?> { [connId.IdValue] = staleHeartbeat });

        var sut = new MasterAgentConnectionService(
            clusterConfig, repo.Object, cache.Object, sentinel.Object, knownEx.Object,
            heartbeat.Object, buckets.Object, locker.Object);

        var all = await sut.QueryAllAsync(useCache: false);

        all.Should().ContainSingle();
        all[0].LastHeartbeatAt.Should().Be(freshCreatedAt);
        all[0].IsAlive().Should().BeTrue();
    }

    [Fact]
    public async Task SafeDeleteConnectionAsync_WhenLockUnavailable_ShouldReturnFalseWithoutDeleting()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = NewRepoMock();
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig, tryLockReturns: null);
        var heartbeat = new Mock<IMasterHeartbeatService>(MockBehavior.Loose);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Loose);
        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Loose);
        var knownEx = new Mock<IKnownExceptionIdentifier>(MockBehavior.Loose);

        var sut = new MasterAgentConnectionService(
            clusterConfig, repo.Object, cache.Object, sentinel.Object, knownEx.Object,
            heartbeat.Object, buckets.Object, locker.Object);

        var connId = new AgentConnectionId(clusterId, "agent");
        var result = await sut.SafeDeleteConnectionAsync(connId);

        result.Should().BeFalse();
        repo.Verify(x => x.DeleteAsync(It.IsAny<string>(), It.IsAny<string>()), Times.Never);
        buckets.Verify(x => x.QueryAsync(It.IsAny<MasterBucketQueryCriteria>(), null), Times.Never);
    }

    [Fact]
    public async Task SafeDeleteConnectionAsync_WhenConnectionHasBuckets_ShouldNotDelete()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = NewRepoMock();
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig, tryLockReturns: LockToken);
        var heartbeat = new Mock<IMasterHeartbeatService>(MockBehavior.Loose);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Loose);
        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Loose);
        var knownEx = new Mock<IKnownExceptionIdentifier>(MockBehavior.Loose);

        var connId = new AgentConnectionId(clusterId, "agent");
        buckets.Setup(x => x.QueryAsync(It.IsAny<MasterBucketQueryCriteria>(), null))
            .ReturnsAsync(new List<BucketModel> { new(clusterId) { Id = "b1" } });

        var sut = new MasterAgentConnectionService(
            clusterConfig, repo.Object, cache.Object, sentinel.Object, knownEx.Object,
            heartbeat.Object, buckets.Object, locker.Object);

        var result = await sut.SafeDeleteConnectionAsync(connId);

        result.Should().BeFalse();
        repo.Verify(x => x.DeleteAsync(It.IsAny<string>(), It.IsAny<string>()), Times.Never);
        locker.Verify(x => x.ReleaseLock(It.IsAny<string>(), LockToken), Times.Once);
    }

    [Fact]
    public async Task SafeDeleteConnectionAsync_WhenConnectionHasNoBuckets_ShouldDeleteAndReleaseLock()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = NewRepoMock();
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig, tryLockReturns: LockToken);
        var heartbeat = new Mock<IMasterHeartbeatService>(MockBehavior.Loose);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Loose);
        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Loose);
        var knownEx = new Mock<IKnownExceptionIdentifier>(MockBehavior.Loose);

        var connId = new AgentConnectionId(clusterId, "agent");
        buckets.Setup(x => x.QueryAsync(It.IsAny<MasterBucketQueryCriteria>(), null))
            .ReturnsAsync(new List<BucketModel>());
        repo.Setup(x => x.DeleteAsync(MasterGenericRecordGroupIds.AgentConnection, connId.IdValue))
            .Returns(Task.CompletedTask);

        var sut = new MasterAgentConnectionService(
            clusterConfig, repo.Object, cache.Object, sentinel.Object, knownEx.Object,
            heartbeat.Object, buckets.Object, locker.Object);

        var result = await sut.SafeDeleteConnectionAsync(connId);

        result.Should().BeTrue();
        repo.Verify(x => x.DeleteAsync(MasterGenericRecordGroupIds.AgentConnection, connId.IdValue), Times.Once);
        locker.Verify(x => x.ReleaseLock(It.IsAny<string>(), LockToken), Times.Once);
    }

    private sealed class AgentConnectionRecordDto
    {
        public string ClusterId { get; set; } = string.Empty;
        public string Id { get; set; } = string.Empty;
        public string Fingerprint { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public DateTime FingerprintCreatedAt { get; set; }
        public string RepositoryTypeId { get; set; } = string.Empty;
        public bool ProtectConnectionChanges { get; set; }
    }

    private static string NewClusterId() => $"c{JobMasterRandomUtil.NewGuid4():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);

    private static Mock<IMasterGenericRecordRepository> NewRepoMock()
        => new(MockBehavior.Loose);

    private static Mock<IMasterChangesSentinelService> NewSentinelMock(JobMasterClusterConnectionConfig cfg)
    {
        var m = new Mock<IMasterChangesSentinelService>(MockBehavior.Loose);
        m.SetupGet(x => x.ClusterConnConfig).Returns(cfg);
        return m;
    }

    private static Mock<IMasterDistributedLockerService> NewLockerMock(JobMasterClusterConnectionConfig cfg, string? tryLockReturns)
    {
        var m = new Mock<IMasterDistributedLockerService>(MockBehavior.Loose);
        m.SetupGet(x => x.ClusterConnConfig).Returns(cfg);
        m.Setup(x => x.TryLock(It.IsAny<string>(), It.IsAny<TimeSpan>())).Returns(tryLockReturns);
        return m;
    }
}
