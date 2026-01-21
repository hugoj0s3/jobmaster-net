using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.BucketSelector;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using Moq;
using JobMaster.Sdk.Services.Master;

namespace JobMaster.UnitTests.Services.Master;

public class MasterBucketsServiceTests
{
    [Fact]
    public void Create_WhenWorkerNotFound_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        clusterConfig.AddAgentConnectionString("agent", "conn", "repo");

        var repo = NewRepoMock(clusterConfig);
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig);
        var workers = NewWorkersMock(clusterConfig);
        var dispatcher = NewDispatcherMock(clusterConfig);
        var masterConfig = NewMasterClusterConfigurationMock(clusterConfig);
        var cache = NewCacheMock();
        var selector = NewSelectorMock();
        var logger = new Mock<IJobMasterLogger>();

        workers.Setup(x => x.GetWorker("w")).Returns((AgentWorkerModel?)null);

        var sut = new MasterBucketsService(
            clusterConfig,
            selector.Object,
            cache.Object,
            repo.Object,
            sentinel.Object,
            locker.Object,
            workers.Object,
            dispatcher.Object,
            masterConfig.Object,
            logger.Object);

        var act = async () => await sut.CreateAsync(new AgentConnectionId(clusterId, "agent"), "w", JobMasterPriority.High);
        act.Should().ThrowAsync<ArgumentException>().WithParameterName("workerId");
    }

    [Fact]
    public void Create_WhenGeneratedBucketIdIsInvalid_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        clusterConfig.AddAgentConnectionString("agent", "conn", "repo");

        var repo = NewRepoMock(clusterConfig);
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig);
        var workers = NewWorkersMock(clusterConfig);
        var dispatcher = NewDispatcherMock(clusterConfig);
        var masterConfig = NewMasterClusterConfigurationMock(clusterConfig);
        var cache = NewCacheMock();
        var selector = NewSelectorMock();
        var logger = new Mock<IJobMasterLogger>();

        var worker = new AgentWorkerModel(clusterId)
        {
            Id = "w",
            Name = "invalid name",
            AgentConnectionId = new AgentConnectionId(clusterId, "agent"),
            WorkerLane = "lane"
        };

        workers.Setup(x => x.GetWorker("w")).Returns(worker);

        var sut = new MasterBucketsService(
            clusterConfig,
            selector.Object,
            cache.Object,
            repo.Object,
            sentinel.Object,
            locker.Object,
            workers.Object,
            dispatcher.Object,
            masterConfig.Object,
            logger.Object);

        var act =  async () => await sut.CreateAsync(new AgentConnectionId(clusterId, "agent"), "w", JobMasterPriority.High);
        act.Should().ThrowAsync<ArgumentException>().WithParameterName("Id");

        repo.Verify(x => x.Insert(It.IsAny<GenericRecordEntry>()), Times.Never);
        dispatcher.Verify(x => x.CreateBucketAsync(It.IsAny<AgentConnectionId>(), It.IsAny<string>()), Times.Never);
        sentinel.Verify(x => x.NotifyChanges(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task Create_ShouldInsertBucket_NotifySentinel_AndDispatchCreateBucket()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        clusterConfig.AddAgentConnectionString("agent", "conn", "repo");

        var repo = NewRepoMock(clusterConfig);
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig);
        var workers = NewWorkersMock(clusterConfig);
        var dispatcher = NewDispatcherMock(clusterConfig);
        var masterConfig = NewMasterClusterConfigurationMock(clusterConfig);
        var cache = NewCacheMock();
        var selector = NewSelectorMock();
        var logger = new Mock<IJobMasterLogger>();

        var worker = new AgentWorkerModel(clusterId)
        {
            Id = "w",
            Name = "worker",
            AgentConnectionId = new AgentConnectionId(clusterId, "agent"),
            WorkerLane = "lane"
        };

        workers.Setup(x => x.GetWorker("w")).Returns(worker);

        GenericRecordEntry? inserted = null;
        repo.Setup(x => x.Insert(It.IsAny<GenericRecordEntry>()))
            .Callback<GenericRecordEntry>(gr => inserted = gr);

        var sut = new MasterBucketsService(
            clusterConfig,
            selector.Object,
            cache.Object,
            repo.Object,
            sentinel.Object,
            locker.Object,
            workers.Object,
            dispatcher.Object,
            masterConfig.Object,
            logger.Object);

        var created = await sut.CreateAsync(new AgentConnectionId(clusterId, "agent"), "w", JobMasterPriority.High);

        created.ClusterId.Should().Be(clusterId);
        created.AgentWorkerId.Should().Be("w");
        created.Priority.Should().Be(JobMasterPriority.High);
        created.Status.Should().Be(BucketStatus.Active);
        created.RepositoryTypeId.Should().Be("repo");
        created.WorkerLane.Should().Be("lane");

        inserted.Should().NotBeNull();
        inserted!.GroupId.Should().Be(MasterGenericRecordGroupIds.Bucket);
        inserted.EntryId.Should().Be(created.Id);

        dispatcher.Verify(x => x.CreateBucketAsync(It.Is<AgentConnectionId>(a => a.IdValue == $"{clusterId}:agent"), created.Id), Times.Once);

        var keys = new JobMasterSentinelKeys(clusterId);
        sentinel.Verify(x => x.NotifyChanges(keys.BucketsAvailableForJobs()), Times.Once);
        sentinel.Verify(x => x.NotifyChanges(keys.Bucket(created.Id)), Times.Once);
    }

    [Fact]
    public async Task Destroy_WhenBucketDoesNotExist_ShouldDoNothing()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = NewRepoMock(clusterConfig);
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig);
        var workers = NewWorkersMock(clusterConfig);
        var dispatcher = NewDispatcherMock(clusterConfig);
        var masterConfig = NewMasterClusterConfigurationMock(clusterConfig);
        var cache = NewCacheMock();
        var selector = NewSelectorMock();
        var logger = new Mock<IJobMasterLogger>();

        repo.Setup(x => x.Get(MasterGenericRecordGroupIds.Bucket, "missing", false)).Returns((GenericRecordEntry?)null);

        var sut = new MasterBucketsService(
            clusterConfig,
            selector.Object,
            cache.Object,
            repo.Object,
            sentinel.Object,
            locker.Object,
            workers.Object,
            dispatcher.Object,
            masterConfig.Object,
            logger.Object);

        await sut.DestroyAsync("missing");

        repo.Verify(x => x.Delete(It.IsAny<string>(), It.IsAny<string>()), Times.Never);
        dispatcher.Verify(x => x.DestroyBucketAsync(It.IsAny<AgentConnectionId>(), It.IsAny<string>()), Times.Never);
        sentinel.Verify(x => x.NotifyChanges(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task Destroy_ReadyToDeleteBucket_ShouldDelete_DestroyOnAgent_AndNotifySentinels()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = NewRepoMock(clusterConfig);
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig);
        var workers = NewWorkersMock(clusterConfig);
        var dispatcher = NewDispatcherMock(clusterConfig);
        var masterConfig = NewMasterClusterConfigurationMock(clusterConfig);
        var cache = NewCacheMock();
        var selector = NewSelectorMock();
        var logger = new Mock<IJobMasterLogger>();

        var agent = new AgentConnectionId(clusterId, "agent");
        var bucket = new BucketModel(clusterId)
        {
            Id = "b1",
            Name = "b1",
            AgentConnectionId = agent,
            AgentWorkerId = "w",
            Priority = JobMasterPriority.High,
            Status = BucketStatus.ReadyToDelete,
            CreatedAt = DateTime.UtcNow,
            RepositoryTypeId = "repo",
            WorkerLane = "lane"
        };

        var entry = GenericRecordEntry.Create(clusterId, MasterGenericRecordGroupIds.Bucket, bucket.Id, bucket);
        repo.Setup(x => x.Get(MasterGenericRecordGroupIds.Bucket, bucket.Id, false)).Returns(entry);

        var sut = new MasterBucketsService(
            clusterConfig,
            selector.Object,
            cache.Object,
            repo.Object,
            sentinel.Object,
            locker.Object,
            workers.Object,
            dispatcher.Object,
            masterConfig.Object,
            logger.Object);

        await sut.DestroyAsync(bucket.Id);

        repo.Verify(x => x.Delete(MasterGenericRecordGroupIds.Bucket, bucket.Id), Times.Once);
        dispatcher.Verify(x => x.DestroyBucketAsync(It.Is<AgentConnectionId>(a => a.IdValue == agent.IdValue), bucket.Id), Times.Once);

        var keys = new JobMasterSentinelKeys(clusterId);
        sentinel.Verify(x => x.NotifyChanges(keys.Bucket(bucket.Id)), Times.Once);
    }

    [Fact]
    public void Get_WhenCacheHasItemAndNoChangesAfter_ShouldReturnCached()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var repo = NewRepoMock(clusterConfig);
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig);
        var workers = NewWorkersMock(clusterConfig);
        var dispatcher = NewDispatcherMock(clusterConfig);
        var masterConfig = NewMasterClusterConfigurationMock(clusterConfig);
        var cache = NewCacheMock();
        var selector = NewSelectorMock();
        var logger = new Mock<IJobMasterLogger>();

        var cachedBucket = new BucketModel(clusterId)
        {
            Id = "b1",
            Name = "b1",
            AgentConnectionId = new AgentConnectionId(clusterId, "agent"),
            Priority = JobMasterPriority.High,
            Status = BucketStatus.Active,
            CreatedAt = DateTime.UtcNow
        };

        var cacheKey = new JobMasterInMemoryKeys(clusterId).Bucket("b1");
        cache.Setup(x => x.Get<BucketModel>(cacheKey))
            .Returns(new JobMasterInMemoryCacheItem<BucketModel>(DateTime.UtcNow, DateTime.UtcNow.AddMinutes(5), cachedBucket));

        var sentinelKey = new JobMasterSentinelKeys(clusterId).Bucket("b1");
        sentinel.Setup(x => x.HasChangesAfter(sentinelKey, It.IsAny<DateTime>(), It.IsAny<TimeSpan?>()))
            .Returns(false);

        var sut = new MasterBucketsService(
            clusterConfig,
            selector.Object,
            cache.Object,
            repo.Object,
            sentinel.Object,
            locker.Object,
            workers.Object,
            dispatcher.Object,
            masterConfig.Object,
            logger.Object);

        var got = sut.Get("b1", JobMasterConstants.BucketDefaultAllowDiscrepancy);

        got.Should().NotBeNull();
        got!.Id.Should().Be("b1");
        repo.Verify(x => x.Get(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>()), Times.Never);
    }

    [Fact]
    public void SelectBucket_WhenCacheMiss_ShouldQueryAssignedBuckets_Filter_AndCache()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        clusterConfig.AddAgentConnectionString("agent", "conn", "repo");

        var repo = NewRepoMock(clusterConfig);
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig);
        var workers = NewWorkersMock(clusterConfig);
        var dispatcher = NewDispatcherMock(clusterConfig);
        var masterConfig = NewMasterClusterConfigurationMock(clusterConfig);
        var cache = NewCacheMock();
        var selector = NewSelectorMock();
        var logger = new Mock<IJobMasterLogger>();

        // Force cache miss
        cache.Setup(x => x.Get<List<BucketModel>>(It.IsAny<string>())).Returns((JobMasterInMemoryCacheItem<List<BucketModel>>?)null);

        var b1 = new BucketModel(clusterId)
        {
            Id = "b1",
            Name = "b1",
            AgentConnectionId = new AgentConnectionId(clusterId, "agent"),
            AgentWorkerId = "w",
            Priority = JobMasterPriority.High,
            Status = BucketStatus.Active,
            CreatedAt = DateTime.UtcNow,
            WorkerLane = "lane"
        };

        var b2 = new BucketModel(clusterId)
        {
            Id = "b2",
            Name = "b2",
            AgentConnectionId = new AgentConnectionId(clusterId, "agent"),
            AgentWorkerId = null,
            Priority = JobMasterPriority.High,
            Status = BucketStatus.Active,
            CreatedAt = DateTime.UtcNow,
            WorkerLane = "lane"
        };

        var b3 = new BucketModel(clusterId)
        {
            Id = "b3",
            Name = "b3",
            AgentConnectionId = new AgentConnectionId(clusterId, "missing"),
            AgentWorkerId = "w",
            Priority = JobMasterPriority.High,
            Status = BucketStatus.Active,
            CreatedAt = DateTime.UtcNow,
            WorkerLane = "lane"
        };

        repo.Setup(x => x.Query(MasterGenericRecordGroupIds.Bucket, It.IsAny<GenericRecordQueryCriteria>()))
            .Returns(new List<GenericRecordEntry>
            {
                GenericRecordEntry.Create(clusterId, MasterGenericRecordGroupIds.Bucket, b1.Id, b1),
                GenericRecordEntry.Create(clusterId, MasterGenericRecordGroupIds.Bucket, b2.Id, b2),
                GenericRecordEntry.Create(clusterId, MasterGenericRecordGroupIds.Bucket, b3.Id, b3)
            });

        selector.Setup(x => x.Select(It.IsAny<IList<BucketModel>>()))
            .Returns<IList<BucketModel>>(lst => lst.FirstOrDefault());

        var sut = new MasterBucketsService(
            clusterConfig,
            selector.Object,
            cache.Object,
            repo.Object,
            sentinel.Object,
            locker.Object,
            workers.Object,
            dispatcher.Object,
            masterConfig.Object,
            logger.Object);

        var selected = sut.SelectBucket(JobMasterConstants.BucketDefaultAllowDiscrepancy, JobMasterPriority.High, "lane");

        selected.Should().NotBeNull();
        selected!.Id.Should().Be("b1");

        cache.Verify(x => x.Set(
            new JobMasterInMemoryKeys(clusterId).BucketsAvailableForJobs(),
            It.Is<List<BucketModel>>(lst => lst.Select(x => x.Id).OrderBy(x => x).SequenceEqual(new[] { "b1", "b3" })),
            null), Times.Once);

        selector.Verify(x => x.Select(It.Is<IList<BucketModel>>(lst => lst.Count == 1 && lst[0].Id == "b1")), Times.Once);
    }

    [Fact]
    public async Task SelectBucketAsync_WhenCacheMiss_ShouldQueryAssignedBuckets_Filter_AndCache()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        clusterConfig.AddAgentConnectionString("agent", "conn", "repo");

        var repo = NewRepoMock(clusterConfig);
        var sentinel = NewSentinelMock(clusterConfig);
        var locker = NewLockerMock(clusterConfig);
        var workers = NewWorkersMock(clusterConfig);
        var dispatcher = NewDispatcherMock(clusterConfig);
        var masterConfig = NewMasterClusterConfigurationMock(clusterConfig);
        var cache = NewCacheMock();
        var selector = NewSelectorMock();
        var logger = new Mock<IJobMasterLogger>();

        // Force cache miss
        cache.Setup(x => x.Get<List<BucketModel>>(It.IsAny<string>())).Returns((JobMasterInMemoryCacheItem<List<BucketModel>>?)null);

        var b1 = new BucketModel(clusterId)
        {
            Id = "b1",
            Name = "b1",
            AgentConnectionId = new AgentConnectionId(clusterId, "agent"),
            AgentWorkerId = "w",
            Priority = JobMasterPriority.High,
            Status = BucketStatus.Active,
            CreatedAt = DateTime.UtcNow,
            WorkerLane = "lane"
        };

        var b2 = new BucketModel(clusterId)
        {
            Id = "b2",
            Name = "b2",
            AgentConnectionId = new AgentConnectionId(clusterId, "agent"),
            AgentWorkerId = null,
            Priority = JobMasterPriority.High,
            Status = BucketStatus.Active,
            CreatedAt = DateTime.UtcNow,
            WorkerLane = "lane"
        };

        var b3 = new BucketModel(clusterId)
        {
            Id = "b3",
            Name = "b3",
            AgentConnectionId = new AgentConnectionId(clusterId, "missing"),
            AgentWorkerId = "w",
            Priority = JobMasterPriority.High,
            Status = BucketStatus.Active,
            CreatedAt = DateTime.UtcNow,
            WorkerLane = "lane"
        };

        repo.Setup(x => x.Query(MasterGenericRecordGroupIds.Bucket, It.IsAny<GenericRecordQueryCriteria>()))
            .Returns(new List<GenericRecordEntry>
            {
                GenericRecordEntry.Create(clusterId, MasterGenericRecordGroupIds.Bucket, b1.Id, b1),
                GenericRecordEntry.Create(clusterId, MasterGenericRecordGroupIds.Bucket, b2.Id, b2),
                GenericRecordEntry.Create(clusterId, MasterGenericRecordGroupIds.Bucket, b3.Id, b3)
            });

        selector.Setup(x => x.Select(It.IsAny<IList<BucketModel>>()))
            .Returns<IList<BucketModel>>(lst => lst.FirstOrDefault());

        var sut = new MasterBucketsService(
            clusterConfig,
            selector.Object,
            cache.Object,
            repo.Object,
            sentinel.Object,
            locker.Object,
            workers.Object,
            dispatcher.Object,
            masterConfig.Object,
            logger.Object);

        var selected = await sut.SelectBucketAsync(JobMasterConstants.BucketDefaultAllowDiscrepancy, JobMasterPriority.High, "lane");

        selected.Should().NotBeNull();
        selected!.Id.Should().Be("b1");

        cache.Verify(x => x.Set(
            new JobMasterInMemoryKeys(clusterId).BucketsAvailableForJobs(),
            It.Is<List<BucketModel>>(lst => lst.Select(x => x.Id).OrderBy(x => x).SequenceEqual(new[] { "b1", "b3" })),
            null), Times.Once);

        selector.Verify(x => x.Select(It.Is<IList<BucketModel>>(lst => lst.Count == 1 && lst[0].Id == "b1")), Times.Once);
    }

    private static string NewClusterId() => $"c{Guid.NewGuid():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);

    private static Mock<IMasterGenericRecordRepository> NewRepoMock(JobMasterClusterConnectionConfig cfg)
    {
        var m = new Mock<IMasterGenericRecordRepository>(MockBehavior.Loose);
        m.SetupGet(x => x.ClusterConnConfig).Returns(cfg);
        m.SetupGet(x => x.MasterRepoTypeId).Returns("fake");
        return m;
    }

    private static Mock<IMasterChangesSentinelService> NewSentinelMock(JobMasterClusterConnectionConfig cfg)
    {
        var m = new Mock<IMasterChangesSentinelService>(MockBehavior.Loose);
        m.SetupGet(x => x.ClusterConnConfig).Returns(cfg);
        return m;
    }

    private static Mock<IMasterDistributedLockerService> NewLockerMock(JobMasterClusterConnectionConfig cfg)
    {
        var m = new Mock<IMasterDistributedLockerService>(MockBehavior.Loose);
        m.SetupGet(x => x.ClusterConnConfig).Returns(cfg);
        return m;
    }

    private static Mock<IMasterAgentWorkersService> NewWorkersMock(JobMasterClusterConnectionConfig cfg)
    {
        var m = new Mock<IMasterAgentWorkersService>(MockBehavior.Loose);
        m.SetupGet(x => x.ClusterConnConfig).Returns(cfg);
        return m;
    }

    private static Mock<IAgentJobsDispatcherService> NewDispatcherMock(JobMasterClusterConnectionConfig cfg)
    {
        var m = new Mock<IAgentJobsDispatcherService>(MockBehavior.Loose);
        m.SetupGet(x => x.ClusterConnConfig).Returns(cfg);
        return m;
    }

    private static Mock<IMasterClusterConfigurationService> NewMasterClusterConfigurationMock(JobMasterClusterConnectionConfig cfg)
    {
        var m = new Mock<IMasterClusterConfigurationService>(MockBehavior.Loose);
        m.SetupGet(x => x.ClusterConnConfig).Returns(cfg);
        return m;
    }

    private static Mock<IJobMasterInMemoryCache> NewCacheMock()
        => new(MockBehavior.Loose);

    private static Mock<IBucketSelectorAlgorithm> NewSelectorMock()
        => new(MockBehavior.Loose);
}
