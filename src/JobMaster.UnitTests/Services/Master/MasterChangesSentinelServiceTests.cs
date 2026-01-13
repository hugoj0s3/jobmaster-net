using FluentAssertions;
using Moq;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.LocalCache;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Services.Master;

namespace JobMaster.UnitTests.Services.Master;

public class MasterChangesSentinelServiceTests
{
    [Fact]
    public void HasChangesAfter_WhenCacheHasNewerValue_ShouldReturnTrue_AndNotHitRepo()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        var sentinelKey = new JobMasterSentinelKeys(clusterId).BucketsAvailableForJobs();

        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Strict);
        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Strict);
        var runtime = new Mock<IJobMasterRuntime>(MockBehavior.Strict);

        runtime.Setup(x => x.IsOnWarmUpTime()).Returns(false);

        var now = DateTime.UtcNow;
        cache.Setup(x => x.Get<DateTime?>(sentinelKey))
            .Returns(new JobMasterInMemoryCacheItem<DateTime?>(createdAt: now.AddSeconds(-1), expiresAt: now.AddMinutes(5), valueObj: now));

        var sut = new MasterChangesSentinelService(clusterConfig, cache.Object, repo.Object);

        var result = sut.HasChangesAfter(sentinelKey, lastUpdate: now.AddMinutes(-5));

        result.Should().BeTrue();

        repo.VerifyNoOtherCalls();
        cache.VerifyAll();
    }

    [Fact]
    public void HasChangesAfter_WhenCacheIsFreshWithinDiscrepancy_ShouldReturnFalse_AndNotHitRepo()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        var sentinelKey = new JobMasterSentinelKeys(clusterId).BucketsAvailableForJobs();

        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Strict);
        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Strict);
        var runtime = new Mock<IJobMasterRuntime>(MockBehavior.Strict);

        runtime.Setup(x => x.IsOnWarmUpTime()).Returns(false);

        var cachedLastUpdate = DateTime.UtcNow.AddMinutes(-10);
        var cachedCreatedAt = DateTime.UtcNow.AddSeconds(-1);

        cache.Setup(x => x.Get<DateTime?>(sentinelKey))
            .Returns(new JobMasterInMemoryCacheItem<DateTime?>(createdAt: cachedCreatedAt, expiresAt: DateTime.UtcNow.AddMinutes(5), valueObj: cachedLastUpdate));

        var sut = new MasterChangesSentinelService(clusterConfig, cache.Object, repo.Object);

        var result = sut.HasChangesAfter(sentinelKey, lastUpdate: cachedLastUpdate, allowedDiscrepancy: TimeSpan.FromSeconds(5));

        result.Should().BeFalse();

        repo.VerifyNoOtherCalls();
        cache.VerifyAll();
    }

    [Fact]
    public void HasChangesAfter_WhenCacheMissAndDbHasNewerValue_ShouldCacheDbValue_AndReturnTrue()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        var sentinelKey = new JobMasterSentinelKeys(clusterId).BucketsAvailableForJobs();

        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Strict);
        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Strict);
        var runtime = new Mock<IJobMasterRuntime>(MockBehavior.Strict);

        runtime.Setup(x => x.IsOnWarmUpTime()).Returns(false);
        cache.Setup(x => x.Get<DateTime?>(sentinelKey)).Returns((JobMasterInMemoryCacheItem<DateTime?>?)null);

        var dbUpdate = DateTime.UtcNow.AddMinutes(-1);
        var entry = GenericRecordEntry.Create(
            clusterId,
            MasterGenericRecordGroupIds.Sentinel,
            sentinelKey,
            new SentinelRecordDto { Id = sentinelKey, LastUpdate = dbUpdate });

        repo.Setup(x => x.Get(MasterGenericRecordGroupIds.Sentinel, sentinelKey, false)).Returns(entry);
        cache.Setup(x => x.Set(sentinelKey, It.Is<DateTime?>(dt => dt == dbUpdate), null));

        var sut = new MasterChangesSentinelService(clusterConfig, cache.Object, repo.Object);

        var result = sut.HasChangesAfter(sentinelKey, lastUpdate: dbUpdate.AddMinutes(-10));

        result.Should().BeTrue();

        cache.VerifyAll();
        repo.VerifyAll();
    }

    [Fact]
    public void HasChangesAfter_WhenCacheMissAndDbHasNoValue_ShouldRemoveCache_AndReturnFalse()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        var sentinelKey = new JobMasterSentinelKeys(clusterId).BucketsAvailableForJobs();

        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Strict);
        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Strict);
        var runtime = new Mock<IJobMasterRuntime>(MockBehavior.Strict);

        runtime.Setup(x => x.IsOnWarmUpTime()).Returns(false);
        cache.Setup(x => x.Get<DateTime?>(sentinelKey)).Returns((JobMasterInMemoryCacheItem<DateTime?>?)null);

        repo.Setup(x => x.Get(MasterGenericRecordGroupIds.Sentinel, sentinelKey, false)).Returns((GenericRecordEntry?)null);
        cache.Setup(x => x.Remove(sentinelKey));

        var sut = new MasterChangesSentinelService(clusterConfig, cache.Object, repo.Object);

        var result = sut.HasChangesAfter(sentinelKey, lastUpdate: DateTime.UtcNow.AddDays(-1));

        result.Should().BeFalse();

        cache.VerifyAll();
        repo.VerifyAll();
    }

    [Fact]
    public void NotifyChanges_WhenKeyIsInvalid_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Strict);
        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Strict);
        var runtime = new Mock<IJobMasterRuntime>(MockBehavior.Strict);

        var sut = new MasterChangesSentinelService(clusterConfig, cache.Object, repo.Object);

        var act = () => sut.NotifyChanges("not-a-valid-key", DateTime.UtcNow);
        act.Should().Throw<ArgumentException>().WithParameterName("key");
    }

    [Fact]
    public void NotifyChanges_ShouldRemoveCache_AndUpsertSentinelRecord()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        var sentinelKey = new JobMasterSentinelKeys(clusterId).BucketsAvailableForJobs();

        var cache = new Mock<IJobMasterInMemoryCache>(MockBehavior.Strict);
        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Strict);
        var runtime = new Mock<IJobMasterRuntime>(MockBehavior.Strict);

        cache.Setup(x => x.Remove(sentinelKey));

        GenericRecordEntry? upserted = null;
        repo.Setup(x => x.Upsert(It.IsAny<GenericRecordEntry>()))
            .Callback<GenericRecordEntry>(e => upserted = e);

        var sut = new MasterChangesSentinelService(clusterConfig, cache.Object, repo.Object);

        var update = new DateTime(2025, 1, 2, 3, 4, 5, DateTimeKind.Local);
        sut.NotifyChanges(sentinelKey, update);

        upserted.Should().NotBeNull();
        upserted!.GroupId.Should().Be(MasterGenericRecordGroupIds.Sentinel);
        upserted.EntryId.Should().Be(sentinelKey);

        upserted.Values.Should().ContainKey("Id");
        upserted.Values.Should().ContainKey("LastUpdate");

        var restored = upserted.ToObject<SentinelRecordDto>();
        restored.Id.Should().Be(sentinelKey);
        restored.LastUpdate.Kind.Should().Be(DateTimeKind.Utc);
        restored.LastUpdate.Should().Be(update.ToUniversalTime());

        cache.VerifyAll();
        repo.VerifyAll();
    }

    private sealed class SentinelRecordDto
    {
        public string Id { get; set; } = string.Empty;
        public DateTime LastUpdate { get; set; }
    }

    private static string NewClusterId() => $"c{Guid.NewGuid():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);
}
