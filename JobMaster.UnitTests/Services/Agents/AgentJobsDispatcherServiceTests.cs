using FluentAssertions;
using Moq;
using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Models.Agents;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Repositories.Agent;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Services.Agents;

namespace JobMaster.UnitTests.Services.Agents;

public class AgentJobsDispatcherServiceTests
{
    [Fact]
    public void AddSavePendingJob_WhenJobNotAssignedToBucket_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        clusterConfig.AddAgentConnectionString("agent", "conn", "repo");
        clusterConfig.Activate();

        var factory = new Mock<IAgentJobsDispatcherRepositoryFactory>(MockBehavior.Strict);
        var sut = new AgentJobsDispatcherService(clusterConfig, factory.Object, new FakeRuntime(true), new Mock<IJobMasterLogger>().Object);

        var job = new JobRawModel(clusterId)
        {
            Id = Guid.NewGuid(),
            JobDefinitionId = "def",
            ScheduledAt = DateTime.UtcNow,
            OriginalScheduledAt = DateTime.UtcNow,
            Priority = JobMasterPriority.High,
            Status = JobMasterJobStatus.HeldOnMaster,
            Timeout = TimeSpan.FromSeconds(1),
            MaxNumberOfRetries = 0,
            AgentConnectionId = null,
            BucketId = null,
        };

        var act = () => sut.AddSavePendingJob(job);
        act.Should().Throw<InvalidOperationException>();

        factory.VerifyNoOtherCalls();
    }

    [Fact]
    public void AddSavePendingJob_WhenAssigned_ShouldPushToRepository()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        clusterConfig.AddAgentConnectionString("agent", "conn", "repo");
        clusterConfig.Activate();

        var agentConnId = new AgentConnectionId(clusterId, "agent");
        
        var bucketId = "b1";

        var repo = new Mock<IAgentJobsDispatcherRepository>(MockBehavior.Strict);
        repo.SetupGet(x => x.AgentRepoTypeId).Returns("repo");
        repo.SetupGet(x => x.IsAutoDequeueForSaving).Returns(false);
        repo.SetupGet(x => x.IsAutoDequeueForProcessing).Returns(false);
        repo.Setup(x => x.PushSavePendingJob(It.IsAny<JobRawModel>()));

        var factory = new Mock<IAgentJobsDispatcherRepositoryFactory>(MockBehavior.Strict);
        factory.Setup(x => x.GetRepository(It.Is<AgentConnectionId>(a => a.IdValue == agentConnId.IdValue))).Returns(repo.Object);

        var sut = new AgentJobsDispatcherService(clusterConfig, factory.Object, new FakeRuntime(true), new Mock<IJobMasterLogger>().Object);

        var job = new JobRawModel(clusterId)
        {
            Id = Guid.NewGuid(),
            JobDefinitionId = "def",
            ScheduledAt = DateTime.UtcNow,
            OriginalScheduledAt = DateTime.UtcNow,
            Priority = JobMasterPriority.High,
            Status = JobMasterJobStatus.SavePending,
            Timeout = TimeSpan.FromSeconds(1),
            MaxNumberOfRetries = 0,
            AgentConnectionId = agentConnId,
            BucketId = bucketId,
        };

        sut.AddSavePendingJob(job);

        repo.Verify(x => x.PushSavePendingJob(It.Is<JobRawModel>(j => j.Id == job.Id)), Times.Once);
        factory.Verify(x => x.GetRepository(It.Is<AgentConnectionId>(a => a.IdValue == agentConnId.IdValue)), Times.Once);
        repo.Verify(x => x.PushSavePendingJob(It.Is<JobRawModel>(j => j.Id == job.Id)), Times.Once);
    }

    [Fact]
    public void AddToProcessing_ShouldAssignToBucket_AndPushToRepository()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        clusterConfig.AddAgentConnectionString("agent", "conn", "repo");
        clusterConfig.Activate();

        var agentConnId = new AgentConnectionId(clusterId, "agent");
        var bucketId = "b1";
        var workerId = "w1";

        var repo = new Mock<IAgentJobsDispatcherRepository>(MockBehavior.Strict);
        repo.SetupGet(x => x.AgentRepoTypeId).Returns("repo");
        repo.SetupGet(x => x.IsAutoDequeueForSaving).Returns(false);
        repo.SetupGet(x => x.IsAutoDequeueForProcessing).Returns(false);
        repo.Setup(x => x.PushToProcessing(It.IsAny<JobRawModel>()));

        var factory = new Mock<IAgentJobsDispatcherRepositoryFactory>(MockBehavior.Strict);
        factory.Setup(x => x.GetRepository(It.Is<AgentConnectionId>(a => a.IdValue == agentConnId.IdValue))).Returns(repo.Object);

        var sut = new AgentJobsDispatcherService(clusterConfig, factory.Object, new FakeRuntime(true), new Mock<IJobMasterLogger>().Object);

        var job = new JobRawModel(clusterId)
        {
            Id = Guid.NewGuid(),
            JobDefinitionId = "def",
            ScheduledAt = DateTime.UtcNow,
            OriginalScheduledAt = DateTime.UtcNow,
            Priority = JobMasterPriority.High,
            Status = JobMasterJobStatus.HeldOnMaster,
            Timeout = TimeSpan.FromSeconds(1),
            MaxNumberOfRetries = 0,
        };

        sut.AddToProcessing(workerId, agentConnId, bucketId, job);

        job.Status.Should().Be(JobMasterJobStatus.AssignedToBucket);
        job.BucketId.Should().Be(bucketId);
        job.AgentWorkerId.Should().Be(workerId);
        job.AgentConnectionId.Should().NotBeNull();
        job.AgentConnectionId!.IdValue.Should().Be(agentConnId.IdValue);

        repo.Verify(x => x.PushToProcessing(It.Is<JobRawModel>(j => j.Id == job.Id && j.BucketId == bucketId)), Times.Once);
        factory.Verify(x => x.GetRepository(It.Is<AgentConnectionId>(a => a.IdValue == agentConnId.IdValue)), Times.Once);
    }

    [Fact]
    public async Task BulkAddSavePendingJobAsync_WhenMoreThanMaxBatchSize_ShouldPartitionCalls()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        clusterConfig.AddAgentConnectionString("agent", "conn", "repo");
        clusterConfig.Activate();

        var agentConnId = new AgentConnectionId(clusterId, "agent");
        var bucketId = "b1";

        var repo = new Mock<IAgentJobsDispatcherRepository>(MockBehavior.Strict);
        repo.SetupGet(x => x.AgentRepoTypeId).Returns("repo");
        repo.SetupGet(x => x.IsAutoDequeueForSaving).Returns(false);
        repo.SetupGet(x => x.IsAutoDequeueForProcessing).Returns(false);

        repo.Setup(x => x.BulkPushSavePendingJobAsync(bucketId, It.IsAny<IList<JobRawModel>>()))
            .Returns(() => Task.FromResult(new List<string>()));

        var factory = new Mock<IAgentJobsDispatcherRepositoryFactory>(MockBehavior.Strict);
        factory.Setup(x => x.GetRepository(It.Is<AgentConnectionId>(a => a.IdValue == agentConnId.IdValue))).Returns(repo.Object);

        var sut = new AgentJobsDispatcherService(clusterConfig, factory.Object, new FakeRuntime(true), new Mock<IJobMasterLogger>().Object);

        var jobs = Enumerable.Range(0, JobMasterConstants.MaxBatchSizeForBulkOperation + 1)
            .Select(_ => new JobRawModel(clusterId)
            {
                Id = Guid.NewGuid(),
                JobDefinitionId = "def",
                ScheduledAt = DateTime.UtcNow,
                OriginalScheduledAt = DateTime.UtcNow,
                Priority = JobMasterPriority.High,
                Status = JobMasterJobStatus.HeldOnMaster,
                Timeout = TimeSpan.FromSeconds(1),
                MaxNumberOfRetries = 0,
                AgentConnectionId = agentConnId,
                BucketId = bucketId,
            })
            .ToList();

        await sut.BulkAddSavePendingJobAsync(jobs);

        // 51 jobs with MaxBatchSize=50 => 2 calls
        repo.Verify(x => x.BulkPushSavePendingJobAsync(bucketId, It.IsAny<IList<JobRawModel>>()), Times.Exactly(2));
        factory.Verify(x => x.GetRepository(It.Is<AgentConnectionId>(a => a.IdValue == agentConnId.IdValue)), Times.Once);
    }

    private static string NewClusterId() => $"c{Guid.NewGuid():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);
}
