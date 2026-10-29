using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Services;
using JobMaster.Sdk.Utils;
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
                m.AgentConnectionId.IdValue == agentConnId.IdValue), It.IsAny<OperationThrottler>()))
            .Returns("job-id-1")
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

        var result = sut.Schedule(job);

        result.Should().BeSameAs(bucket);
        dispatcher.Verify();
        buckets.Verify();
        jobs.Verify(x => x.Add(It.IsAny<JobRawModel>()), Times.Never);
    }

    [Fact]
    public void Schedule_WhenBucketMissing_ShouldHoldOnMaster_AndAdd()
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
            .Setup(x => x.Add(It.Is<JobRawModel>(m => m.Status == JobMasterJobStatus.OnMaster)))
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

        var result = sut.Schedule(job);

        result.Should().BeNull();
        jobs.Verify();
        dispatcher.Verify(x => x.AddSavePendingJob(It.IsAny<JobRawModel>(), It.IsAny<OperationThrottler>()), Times.Never);
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
    public async Task ScheduleAsync_WhenClusterModeMigrating_ShouldHoldOnMaster_AndAddAsync()
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
            ClusterMode = ClusterMode.Migrating,
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
            .Setup(x => x.AddAsync(It.Is<JobRawModel>(m => m.Status == JobMasterJobStatus.OnMaster)))
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

        var result = await sut.ScheduleAsync(NewJob(clusterId));

        result.Should().BeNull();
        jobs.Verify();
        dispatcher.Verify(x => x.AddSavePendingJobAsync(It.IsAny<JobRawModel>(), It.IsAny<OperationThrottler>()), Times.Never);
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
            Id = JobMasterRandomUtil.NewGuid4(),
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
    public void Schedule_WhenClusterModeMigrating_ShouldHoldOnMaster_AndAdd()
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
            ClusterMode = ClusterMode.Migrating,
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
            .Setup(x => x.Add(It.Is<JobRawModel>(m => m.Status == JobMasterJobStatus.OnMaster)))
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

        var result = sut.Schedule(NewJob(clusterId));

        result.Should().BeNull();
        jobs.Verify();
        dispatcher.Verify(x => x.AddSavePendingJob(It.IsAny<JobRawModel>(), It.IsAny<OperationThrottler>()), Times.Never);
    }

    [Fact]
    public async Task ScheduleAsyncRecurring_WhenBucketAvailable_ReturnsBucket()
    {
        var clusterId = NewClusterId();
        var mocks = new Mocks(clusterId, ClusterMode.Active);
        var bucket = NewBucket(clusterId);
        mocks.Buckets
            .Setup(x => x.SelectBucketAsync(It.IsAny<TimeSpan?>(), JobMasterPriority.Medium, null))
            .ReturnsAsync(bucket);
        mocks.Dispatcher
            .Setup(x => x.AddSavePendingRecurAsync(It.IsAny<RecurringScheduleRawModel>(), It.IsAny<OperationThrottler>()))
            .ReturnsAsync("recur-id");

        var result = await mocks.CreateSut().ScheduleAsync(NewRecurring(clusterId));

        result.Should().BeSameAs(bucket);
    }

    [Fact]
    public async Task ScheduleAsyncRecurring_WhenBucketMissing_AddsActiveOnMaster_AndReturnsNull()
    {
        var clusterId = NewClusterId();
        var mocks = new Mocks(clusterId, ClusterMode.Active);
        mocks.Buckets
            .Setup(x => x.SelectBucketAsync(It.IsAny<TimeSpan?>(), JobMasterPriority.Medium, null))
            .ReturnsAsync((BucketModel?)null);
        mocks.Recurs
            .Setup(x => x.AddAsync(It.Is<RecurringScheduleRawModel>(m => m.Status == RecurringScheduleStatus.Active)))
            .Returns(Task.CompletedTask)
            .Verifiable();

        var result = await mocks.CreateSut().ScheduleAsync(NewRecurring(clusterId));

        result.Should().BeNull();
        mocks.Recurs.Verify();
    }

    [Fact]
    public async Task ConfirmJobsOnMasterAsync_MarksHeldOnMaster_AndSkipsExistenceCheckWhenAllInserted()
    {
        var clusterId = NewClusterId();
        var mocks = new Mocks(clusterId, ClusterMode.Active);
        var jobList = new List<JobRawModel> { NewJob(clusterId), NewJob(clusterId) };
        mocks.Jobs
            .Setup(x => x.BulkInsertIfNotExistsAsync(It.IsAny<IList<JobRawModel>>()))
            .ReturnsAsync((IList<JobRawModel> batch) => batch.Select(j => j.Id).ToList());

        await mocks.CreateSut().ConfirmJobsOnMasterAsync(jobList);

        jobList.Should().OnlyContain(j => j.Status == JobMasterJobStatus.OnMaster);
        mocks.Jobs.Verify(x => x.QueryExistingIdsAsync(It.IsAny<IList<Guid>>()), Times.Never);
    }

    [Fact]
    public async Task ConfirmJobsOnMasterAsync_WhenNotInsertedButAlreadyOnMaster_Succeeds()
    {
        var clusterId = NewClusterId();
        var mocks = new Mocks(clusterId, ClusterMode.Active);
        var inserted = NewJob(clusterId);
        var alreadyThere = NewJob(clusterId);
        mocks.Jobs
            .Setup(x => x.BulkInsertIfNotExistsAsync(It.IsAny<IList<JobRawModel>>()))
            .ReturnsAsync(new List<Guid> { inserted.Id });
        mocks.Jobs
            .Setup(x => x.QueryExistingIdsAsync(It.Is<IList<Guid>>(ids => ids.SequenceEqual(new[] { alreadyThere.Id }))))
            .ReturnsAsync(new List<Guid> { alreadyThere.Id })
            .Verifiable();

        await mocks.CreateSut().ConfirmJobsOnMasterAsync(new List<JobRawModel> { inserted, alreadyThere });

        mocks.Jobs.Verify();
    }

    [Fact]
    public async Task ConfirmJobsOnMasterAsync_WhenNeitherInsertedNorOnMaster_Throws()
    {
        var clusterId = NewClusterId();
        var mocks = new Mocks(clusterId, ClusterMode.Active);
        var missing = NewJob(clusterId);
        mocks.Jobs
            .Setup(x => x.BulkInsertIfNotExistsAsync(It.IsAny<IList<JobRawModel>>()))
            .ReturnsAsync(new List<Guid>());
        mocks.Jobs
            .Setup(x => x.QueryExistingIdsAsync(It.IsAny<IList<Guid>>()))
            .ReturnsAsync(new List<Guid>());

        var act = () => mocks.CreateSut().ConfirmJobsOnMasterAsync(new List<JobRawModel> { missing });

        (await act.Should().ThrowAsync<InvalidOperationException>())
            .Which.Message.Should().Contain(missing.Id.ToString());
    }

    [Fact]
    public async Task ConfirmJobsOnMasterAsync_PartitionsByMasterMaxBatchSize()
    {
        var clusterId = NewClusterId();
        var mocks = new Mocks(clusterId, ClusterMode.Active);
        var maxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(clusterId);
        var jobList = Enumerable.Range(0, maxBatchSize * 2 + 1).Select(_ => NewJob(clusterId)).ToList();
        var batches = new List<IList<JobRawModel>>();
        mocks.Jobs
            .Setup(x => x.BulkInsertIfNotExistsAsync(It.IsAny<IList<JobRawModel>>()))
            .Callback<IList<JobRawModel>>(batches.Add)
            .ReturnsAsync((IList<JobRawModel> batch) => batch.Select(j => j.Id).ToList());

        await mocks.CreateSut().ConfirmJobsOnMasterAsync(jobList);

        batches.Should().HaveCount(3);
        batches.Should().OnlyContain(b => b.Count <= maxBatchSize);
        batches.SelectMany(b => b).Should().BeEquivalentTo(jobList);
    }

    [Fact]
    public async Task ConfirmJobsOnMasterAsync_WhenClusterArchived_ThrowsWithoutWriting()
    {
        var clusterId = NewClusterId();
        var mocks = new Mocks(clusterId, ClusterMode.Archived); // strict mocks: any write fails the test

        var act = () => mocks.CreateSut().ConfirmJobsOnMasterAsync(new List<JobRawModel> { NewJob(clusterId) });

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("Cluster mode is archived");
    }

    [Fact]
    public async Task ConfirmRecurringSchedulesOnMasterAsync_MarksActive_AndInserts()
    {
        var clusterId = NewClusterId();
        var mocks = new Mocks(clusterId, ClusterMode.Active);
        var schedules = new List<RecurringScheduleRawModel> { NewRecurring(clusterId), NewRecurring(clusterId) };
        mocks.Recurs
            .Setup(x => x.BulkInsertIfNotExistsAsync(It.IsAny<IList<RecurringScheduleRawModel>>()))
            .ReturnsAsync((IList<RecurringScheduleRawModel> batch) => batch.Select(s => s.Id).ToList());

        await mocks.CreateSut().ConfirmRecurringSchedulesOnMasterAsync(schedules);

        schedules.Should().OnlyContain(s => s.Status == RecurringScheduleStatus.Active);
        mocks.Recurs.Verify(x => x.QueryExistingIdsAsync(It.IsAny<IList<Guid>>()), Times.Never);
    }

    [Fact]
    public async Task ConfirmRecurringSchedulesOnMasterAsync_WhenNeitherInsertedNorOnMaster_Throws()
    {
        var clusterId = NewClusterId();
        var mocks = new Mocks(clusterId, ClusterMode.Active);
        var alreadyThere = NewRecurring(clusterId);
        var missing = NewRecurring(clusterId);
        mocks.Recurs
            .Setup(x => x.BulkInsertIfNotExistsAsync(It.IsAny<IList<RecurringScheduleRawModel>>()))
            .ReturnsAsync(new List<Guid>());
        mocks.Recurs
            .Setup(x => x.QueryExistingIdsAsync(It.IsAny<IList<Guid>>()))
            .ReturnsAsync(new List<Guid> { alreadyThere.Id });

        var act = () => mocks.CreateSut().ConfirmRecurringSchedulesOnMasterAsync(
            new List<RecurringScheduleRawModel> { alreadyThere, missing });

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        thrown.Which.Message.Should().Contain(missing.Id.ToString()).And.NotContain(alreadyThere.Id.ToString());
    }

    [Fact]
    public async Task ConfirmRecurringSchedulesOnMasterAsync_WhenClusterArchived_ThrowsWithoutWriting()
    {
        var clusterId = NewClusterId();
        var mocks = new Mocks(clusterId, ClusterMode.Archived);

        var act = () => mocks.CreateSut().ConfirmRecurringSchedulesOnMasterAsync(
            new List<RecurringScheduleRawModel> { NewRecurring(clusterId) });

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("Cluster mode is archived");
    }

    [Fact]
    public void CancelJob_PersistsWithSyncUpdate_BeforeReturning()
    {
        var clusterId = NewClusterId();
        var mocks = new Mocks(clusterId, ClusterMode.Active);
        var job = NewJob(clusterId);
        job.Status = JobMasterJobStatus.OnMaster;
        job.ScheduledAt = DateTime.UtcNow.AddHours(1); // outside the onboarding window
        job.NextPlanExecutionAt = job.ScheduledAt;
        mocks.Jobs.Setup(x => x.Get(job.Id)).Returns(job);
        mocks.Jobs
            .Setup(x => x.Update(It.Is<JobRawModel>(m => m.Id == job.Id && m.Status == JobMasterJobStatus.Cancelled), null, null))
            .Verifiable();

        mocks.CreateSut().CancelJob(job.Id).Should().BeTrue();

        mocks.Jobs.Verify();
        mocks.Jobs.Verify(x => x.UpdateAsync(It.IsAny<JobRawModel>(), It.IsAny<JobExecution?>(), It.IsAny<OperationThrottler?>()), Times.Never);
    }

    private sealed class Mocks
    {
        private readonly JobMasterClusterConnectionConfig clusterConfig;

        public Mock<IAgentJobsDispatcherService> Dispatcher { get; } = new(MockBehavior.Strict);
        public Mock<IMasterBucketsService> Buckets { get; } = new(MockBehavior.Strict);
        public Mock<IMasterJobsService> Jobs { get; } = new(MockBehavior.Strict);
        public Mock<IMasterRecurringSchedulesService> Recurs { get; } = new(MockBehavior.Strict);
        public Mock<IMasterDistributedLockerService> Locker { get; } = new(MockBehavior.Strict);
        public Mock<IMasterClusterConfigurationService> MasterConfig { get; } = new(MockBehavior.Strict);

        public Mocks(string clusterId, ClusterMode mode)
        {
            clusterConfig = CreateClusterConfigWithActiveAgent(clusterId, agentName: "a");
            var model = new ClusterConfigurationModel(clusterId) { ClusterMode = mode, MaxMessageByteSize = 1024 * 1024 };
            MasterConfig.Setup(x => x.Get()).Returns(model);
            MasterConfig.Setup(x => x.GetFresh()).Returns(model);
        }

        public JobMasterSchedulerClusterAware CreateSut() => new(
            clusterConfig,
            Dispatcher.Object,
            Buckets.Object,
            Jobs.Object,
            Recurs.Object,
            Locker.Object,
            MasterConfig.Object,
            new Mock<IJobMasterLogger>().Object);
    }

    private static BucketModel NewBucket(string clusterId) => new(clusterId)
    {
        Id = $"{clusterId}.bucket",
        Name = $"{clusterId}.bucket",
        AgentConnectionId = new AgentConnectionId(clusterId, "a"),
        AgentWorkerId = "w",
        Status = BucketStatus.Active,
        Priority = JobMasterPriority.Medium,
    };

    private static RecurringScheduleRawModel NewRecurring(string clusterId) => new(clusterId)
    {
        Id = JobMasterRandomUtil.NewGuid4(),
        JobDefinitionId = "def",
        MsgData = "{}",
        CreatedAt = DateTime.UtcNow,
        Priority = JobMasterPriority.Medium,
    };

    private static JobRawModel NewJob(string clusterId)
    {
        return new JobRawModel(clusterId)
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            JobDefinitionId = "def",
            TriggerSourceType = JobMasterTriggerSourceType.Once,
            Priority = JobMasterPriority.Medium,
            ScheduledAt = DateTime.UtcNow,
            NextPlanExecutionAt = DateTime.UtcNow,
            Status = JobMasterJobStatus.PendingSave,
            Timeout = TimeSpan.FromSeconds(5),
            MaxNumberOfRetries = 0,
            MsgData = "{}",
            CreatedAt = DateTime.UtcNow,
        };
    }

    private static string NewClusterId() => JobMasterRandomUtil.NewGuid4().ToString("N");

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

        cfg.MarkAsReady();
        return cfg;
    }
}
