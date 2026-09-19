using FluentAssertions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Cache;
using JobMaster.Sdk.Services.Master;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Services.Master;

/// <summary>
/// Reproduces the reported production symptom: a host that has been deleted via
/// <see cref="MasterHostService.DeleteHostsAsync"/> still shows up in
/// <see cref="MasterHostService.QueryAllAsync"/> immediately afterwards, in the same process,
/// suggesting the sentinel-backed cache (<see cref="JobMasterInMemoryCache"/> +
/// <see cref="MasterChangesSentinelService"/>) isn't being invalidated on delete.
/// </summary>
public class MasterHostServiceTests
{
    [Fact]
    public async Task DeleteHostsAsync_ThenQueryAllAsync_SameProcess_ShouldNotReturnDeletedHost()
    {
        var clusterId = $"c{JobMasterRandomUtil.NewGuid4():N}";
        var clusterConfig = JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);

        var store = new Dictionary<string, GenericRecordEntry>();
        var repo = new Mock<IMasterGenericRecordRepository>(MockBehavior.Loose);

        repo.Setup(x => x.InsertAsync(It.IsAny<GenericRecordEntry>()))
            .Callback<GenericRecordEntry>(e => store[$"{e.GroupId}:{e.EntryId}"] = e)
            .Returns(Task.CompletedTask);
        repo.Setup(x => x.Upsert(It.IsAny<GenericRecordEntry>()))
            .Callback<GenericRecordEntry>(e => store[$"{e.GroupId}:{e.EntryId}"] = e);
        repo.Setup(x => x.UpsertAsync(It.IsAny<GenericRecordEntry>()))
            .Callback<GenericRecordEntry>(e => store[$"{e.GroupId}:{e.EntryId}"] = e)
            .Returns(Task.CompletedTask);
        repo.Setup(x => x.DeleteAsync(It.IsAny<string>(), It.IsAny<string>()))
            .Callback<string, string>((groupId, id) => store.Remove($"{groupId}:{id}"))
            .Returns(Task.CompletedTask);
        repo.Setup(x => x.QueryAsync(It.IsAny<string>(), null))
            .ReturnsAsync((string groupId, GenericRecordQueryCriteria? _) =>
                store.Values.Where(v => v.GroupId == groupId).ToList());
        repo.Setup(x => x.Get(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>()))
            .Returns((string groupId, string id, bool _) =>
                store.TryGetValue($"{groupId}:{id}", out var e) ? e : null);

        // Real cache + real sentinel service (not mocked) so the actual invalidation path runs.
        var cache = new JobMasterInMemoryCache();
        var sentinel = new MasterChangesSentinelService(clusterConfig, cache, repo.Object);

        var heartbeat = new Mock<IMasterHeartbeatService>(MockBehavior.Loose);
        heartbeat.Setup(x => x.GetLastHeartbeats(ResourceHeartbeatType.Host, It.IsAny<IList<string>>()))
            .Returns(new Dictionary<string, DateTime?>());

        var knownEx = new Mock<IKnownExceptionIdentifier>(MockBehavior.Loose);
        var statsProvider = new Mock<IHostStatsProvider>(MockBehavior.Loose);
        statsProvider.Setup(x => x.GetStatsAsync()).ReturnsAsync(new HostStatsInfo());

        var sut = new MasterHostService(
            clusterConfig, repo.Object, heartbeat.Object, cache, sentinel, knownEx.Object, statsProvider.Object);

        var hostId = await sut.RegisterNewHostAsync();

        var afterRegister = await sut.QueryAllAsync();
        afterRegister.Should().ContainSingle(h => h.Id.IdValue == hostId.IdValue);

        await sut.DeleteHostsAsync(new List<string> { hostId.IdValue });

        var afterDelete = await sut.QueryAllAsync();
        afterDelete.Should().NotContain(h => h.Id.IdValue == hostId.IdValue);
    }
}
