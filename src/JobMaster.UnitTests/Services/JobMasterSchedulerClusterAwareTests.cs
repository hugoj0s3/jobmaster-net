using FluentAssertions;
using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Models;
using JobMaster.Sdk.Contracts.Models.Agents;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Services.Agent;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Services;
using Moq;

namespace JobMaster.UnitTests.Services;

public class JobMasterSchedulerClusterAwareTests
{
    [Fact]
    public void Schedule_WhenBucketAvailable_ShouldAssignAndDispatchToAgent()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfigWithActiveAgent(clusterId, agentName: "a");

        var agentConnId = new AgentConnectionId(clusterId, "a");
        var bucket = new BucketModel(clusterId)
        {
            Id = $"{clusterId}.bucket",
            Name = $"{clusterId}.bucket",
            AgentConnectionId = agentConnId,
            AgentWorkerId = "w",
            Status = BucketStatus.Active,
            Priority = JobMasterPriority.Medium,
        };

        var dispatcher = new Mock<IAgentJobsDispatcherService>(MockBehavior.Strict);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Strict);
        var jobs = new Mock<IMasterJobsService>(MockBehavior.Strict);
        var recurs = new Mock<IMasterRecurringSchedulesService>(MockBehavior.Strict);
        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Strict);
        var masterConfig = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);

        masterConfig.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId)
        {
            ClusterMode = ClusterMode.Active,
            MaxMessageByteSize = 1024 * 1024
        });
        
        buckets
            .Setup(x => x.SelectBucket(It.IsAny<TimeSpan?>(), JobMasterPriority.Medium, null))
            .Returns(bucket);

        dispatcher
            .Setup(x => x.AddSavePendingJob(It.Is<JobRawModel>(m =>
                m.ClusterId == clusterId &&
                m.BucketId == bucket.Id &&
                m.AgentWorkerId == bucket.AgentWorkerId &&
                m.AgentConnectionId != null &&
                m.AgentConnectionId.IdValue == agentConnId.IdValue)))
            .Verifiable();

        var sut = new JobMasterSchedulerClusterAware(
            clusterConfig,
            dispatcher.Object,
            buckets.Object,
            jobs.Object,
            recurs.Object,
            locker.Object,
            masterConfig.Object,
            new Mock<IJobMasterLogger>().Object);

        var job = NewJob(clusterId);

        sut.Schedule(job);

        dispatcher.Verify();
        buckets.Verify();
        jobs.Verify(x => x.Upsert(It.IsAny<JobRawModel>()), Times.Never);
    }

    [Fact]
    public void Schedule_WhenBucketMissing_ShouldHoldOnMaster_AndUpsert()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfigWithActiveAgent(clusterId, agentName: "a");

        var dispatcher = new Mock<IAgentJobsDispatcherService>(MockBehavior.Strict);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Strict);
        var jobs = new Mock<IMasterJobsService>(MockBehavior.Strict);
        var recurs = new Mock<IMasterRecurringSchedulesService>(MockBehavior.Strict);
        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Strict);
        var masterConfig = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);

        masterConfig.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId)
        {
            ClusterMode = ClusterMode.Active,
            MaxMessageByteSize = 1024 * 1024
        });

        buckets
            .Setup(x => x.SelectBucket(It.IsAny<TimeSpan?>(), JobMasterPriority.Medium, null))
            .Returns(() => null);

        jobs
            .Setup(x => x.Upsert(It.Is<JobRawModel>(m => m.Status == JobMasterJobStatus.HeldOnMaster)))
            .Verifiable();

        var sut = new JobMasterSchedulerClusterAware(
            clusterConfig,
            dispatcher.Object,
            buckets.Object,
            jobs.Object,
            recurs.Object,
            locker.Object,
            masterConfig.Object,
            new Mock<IJobMasterLogger>().Object);

        var job = NewJob(clusterId);

        sut.Schedule(job);

        jobs.Verify();
        dispatcher.Verify(x => x.AddSavePendingJob(It.IsAny<JobRawModel>()), Times.Never);
    }

    [Fact]
    public async Task ScheduleAsync_WhenClusterModeArchived_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfigWithActiveAgent(clusterId, agentName: "a");

        var dispatcher = new Mock<IAgentJobsDispatcherService>(MockBehavior.Strict);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Strict);
        var jobs = new Mock<IMasterJobsService>(MockBehavior.Strict);
        var recurs = new Mock<IMasterRecurringSchedulesService>(MockBehavior.Strict);
        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Strict);
        var masterConfig = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);

        masterConfig.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId)
        {
            ClusterMode = ClusterMode.Archived,
        });

        var sut = new JobMasterSchedulerClusterAware(
            clusterConfig,
            dispatcher.Object,
            buckets.Object,
            jobs.Object,
            recurs.Object,
            locker.Object,
            masterConfig.Object,
            new Mock<IJobMasterLogger>().Object);

        var act = () => sut.ScheduleAsync(NewJob(clusterId));

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Cluster mode is archived");
    }

    [Fact]
    public async Task ScheduleAsync_WhenClusterModePassive_ShouldHoldOnMaster_AndUpsertAsync()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfigWithActiveAgent(clusterId, agentName: "a");

        var dispatcher = new Mock<IAgentJobsDispatcherService>(MockBehavior.Strict);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Strict);
        var jobs = new Mock<IMasterJobsService>(MockBehavior.Strict);
        var recurs = new Mock<IMasterRecurringSchedulesService>(MockBehavior.Strict);
        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Strict);
        var masterConfig = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);

        masterConfig.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId)
        {
            ClusterMode = ClusterMode.Passive,
            MaxMessageByteSize = 1024 * 1024
        });

        buckets
            .Setup(x => x.SelectBucketAsync(It.IsAny<TimeSpan?>(), JobMasterPriority.Medium, null))
            .ReturnsAsync(new BucketModel(clusterId)
            {
                Id = $"{clusterId}.bucket",
                Name = $"{clusterId}.bucket",
                AgentConnectionId = new AgentConnectionId(clusterId, "a"),
                AgentWorkerId = "w",
                Status = BucketStatus.Active,
                Priority = JobMasterPriority.Medium,
            });

        jobs
            .Setup(x => x.UpsertAsync(It.Is<JobRawModel>(m => m.Status == JobMasterJobStatus.HeldOnMaster)))
            .Returns(Task.CompletedTask)
            .Verifiable();

        var sut = new JobMasterSchedulerClusterAware(
            clusterConfig,
            dispatcher.Object,
            buckets.Object,
            jobs.Object,
            recurs.Object,
            locker.Object,
            masterConfig.Object,
            new Mock<IJobMasterLogger>().Object);

        await sut.ScheduleAsync(NewJob(clusterId));

        jobs.Verify();
        dispatcher.Verify(x => x.AddSavePendingJobAsync(It.IsAny<JobRawModel>()), Times.Never);
    }

    [Fact]
    public void Schedule_WhenHoldingOnMaster_AndEstimatedSizeExceedsMax_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfigWithActiveAgent(clusterId, agentName: "a");

        var dispatcher = new Mock<IAgentJobsDispatcherService>(MockBehavior.Strict);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Strict);
        var jobs = new Mock<IMasterJobsService>(MockBehavior.Strict);
        var recurs = new Mock<IMasterRecurringSchedulesService>(MockBehavior.Strict);
        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Strict);
        var masterConfig = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);

        masterConfig.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId)
        {
            ClusterMode = ClusterMode.Active,
            MaxMessageByteSize = 1
        });

        buckets
            .Setup(x => x.SelectBucket(It.IsAny<TimeSpan?>(), JobMasterPriority.Medium, null))
            .Returns(() => null);

        var sut = new JobMasterSchedulerClusterAware(
            clusterConfig,
            dispatcher.Object,
            buckets.Object,
            jobs.Object,
            recurs.Object,
            locker.Object,
            masterConfig.Object,
            new Mock<IJobMasterLogger>().Object);

        var act = () => sut.Schedule(NewJob(clusterId));

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void ScheduleRecurring_WhenHoldingOnMaster_AndEstimatedSizeExceedsMax_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfigWithActiveAgent(clusterId, agentName: "a");

        var dispatcher = new Mock<IAgentJobsDispatcherService>(MockBehavior.Strict);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Strict);
        var jobs = new Mock<IMasterJobsService>(MockBehavior.Strict);
        var recurs = new Mock<IMasterRecurringSchedulesService>(MockBehavior.Strict);
        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Strict);
        var masterConfig = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);

        masterConfig.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId)
        {
            ClusterMode = ClusterMode.Active,
            MaxMessageByteSize = 1
        });

        buckets
            .Setup(x => x.SelectBucket(It.IsAny<TimeSpan?>(), JobMasterPriority.Medium, null))
            .Returns(() => null);

        var sut = new JobMasterSchedulerClusterAware(
            clusterConfig,
            dispatcher.Object,
            buckets.Object,
            jobs.Object,
            recurs.Object,
            locker.Object,
            masterConfig.Object,
            new Mock<IJobMasterLogger>().Object);

        var raw = new RecurringScheduleRawModel(clusterId)
        {
            Id = Guid.NewGuid(),
            JobDefinitionId = "def",
            MsgData = "{}",
            CreatedAt = DateTime.UtcNow,
            Priority = JobMasterPriority.Medium
        };

        var act = () => sut.Schedule(raw);

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Schedule_WhenClusterModeArchived_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfigWithActiveAgent(clusterId, agentName: "a");

        var dispatcher = new Mock<IAgentJobsDispatcherService>(MockBehavior.Strict);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Strict);
        var jobs = new Mock<IMasterJobsService>(MockBehavior.Strict);
        var recurs = new Mock<IMasterRecurringSchedulesService>(MockBehavior.Strict);
        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Strict);
        var masterConfig = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);

        masterConfig.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId)
        {
            ClusterMode = ClusterMode.Archived,
        });

        var sut = new JobMasterSchedulerClusterAware(
            clusterConfig,
            dispatcher.Object,
            buckets.Object,
            jobs.Object,
            recurs.Object,
            locker.Object,
            masterConfig.Object,
            new Mock<IJobMasterLogger>().Object);

        var act = () => sut.Schedule(NewJob(clusterId));

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("Cluster mode is archived");
    }

    [Fact]
    public void Schedule_WhenClusterModePassive_ShouldHoldOnMaster_AndUpsert()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfigWithActiveAgent(clusterId, agentName: "a");

        var dispatcher = new Mock<IAgentJobsDispatcherService>(MockBehavior.Strict);
        var buckets = new Mock<IMasterBucketsService>(MockBehavior.Strict);
        var jobs = new Mock<IMasterJobsService>(MockBehavior.Strict);
        var recurs = new Mock<IMasterRecurringSchedulesService>(MockBehavior.Strict);
        var locker = new Mock<IMasterDistributedLockerService>(MockBehavior.Strict);
        var masterConfig = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);

        masterConfig.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId)
        {
            ClusterMode = ClusterMode.Passive,
            MaxMessageByteSize = 1024 * 1024
        });

        buckets
            .Setup(x => x.SelectBucket(It.IsAny<TimeSpan?>(), JobMasterPriority.Medium, null))
            .Returns(new BucketModel(clusterId)
            {
                Id = $"{clusterId}.bucket",
                Name = $"{clusterId}.bucket",
                AgentConnectionId = new AgentConnectionId(clusterId, "a"),
                AgentWorkerId = "w",
                Status = BucketStatus.Active,
                Priority = JobMasterPriority.Medium,
            });

        jobs
            .Setup(x => x.Upsert(It.Is<JobRawModel>(m => m.Status == JobMasterJobStatus.HeldOnMaster)))
            .Verifiable();

        var sut = new JobMasterSchedulerClusterAware(
            clusterConfig,
            dispatcher.Object,
            buckets.Object,
            jobs.Object,
            recurs.Object,
            locker.Object,
            masterConfig.Object,
            new Mock<IJobMasterLogger>().Object);

        sut.Schedule(NewJob(clusterId));

        jobs.Verify();
        dispatcher.Verify(x => x.AddSavePendingJob(It.IsAny<JobRawModel>()), Times.Never);
    }

    private static JobRawModel NewJob(string clusterId)
    {
        return new JobRawModel(clusterId)
        {
            Id = Guid.NewGuid(),
            JobDefinitionId = "def",
            ScheduleSourceType = JobSchedulingSourceType.Once,
            Priority = JobMasterPriority.Medium,
            OriginalScheduledAt = DateTime.UtcNow,
            ScheduledAt = DateTime.UtcNow,
            Status = JobMasterJobStatus.SavePending,
            Timeout = TimeSpan.FromSeconds(5),
            MaxNumberOfRetries = 0,
            MsgData = "{}",
            CreatedAt = DateTime.UtcNow,
        };
    }

    private static string NewClusterId() => Guid.NewGuid().ToString("N");

    private static JobMasterClusterConnectionConfig CreateClusterConfigWithActiveAgent(string clusterId, string agentName)
    {
        var cfg = JobMasterClusterConnectionConfig.Create(
            clusterId: clusterId,
            repositoryTypeId: "repo",
            connectionString: "cnn",
            isDefault: false);

        cfg.AddAgentConnectionString(
            name: agentName,
            connectionString: "cnn-agent",
            repositoryTypeId: "repo-agent");

        cfg.Activate();
        return cfg;
    }
}
