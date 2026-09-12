using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using Moq;
using JobMaster.Sdk.Services.Agents;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Services.Agents;

public class AgentJobsDispatcherServiceTests
{
    [Fact]
    public void AddSavePendingJob_WhenJobNotAssignedToBucket_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        clusterConfig.AddAgentConnectionString("agent", "conn", "repo");
        clusterConfig.MarkAsReady();

        var factory = new Mock<IAgentComponentFactory>(MockBehavior.Strict);
        var sut = new AgentJobsDispatcherService(clusterConfig, factory.Object, new Mock<IJobMasterLogger>().Object);

        var job = new JobRawModel(clusterId)
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            JobDefinitionId = "def",
            NextPlanExecutionAt = DateTime.UtcNow,
            ScheduledAt = DateTime.UtcNow,
            Priority = JobMasterPriority.High,
            Status = JobMasterJobStatus.OnMaster,
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
        clusterConfig.MarkAsReady();

        var agentConnId = new AgentConnectionId(clusterId, "agent");
        
        var bucketId = "b1";

        var repo = new Mock<IAgentJobsDispatcherRepository>(MockBehavior.Strict);
        repo.SetupGet(x => x.AgentRepoTypeId).Returns("repo");
        repo.SetupGet(x => x.IsPollingBasedForSaving).Returns(true);
        repo.SetupGet(x => x.IsPollingBasedForProcessing).Returns(true);
        repo.Setup(x => x.PushSavePendingJob(It.IsAny<JobRawModel>()))
            .Returns("job-id-1");

        var factory = new Mock<IAgentComponentFactory>(MockBehavior.Strict);
        factory.Setup(x => x.GetRepository(It.Is<AgentConnectionId>(a => a.IdValue == agentConnId.IdValue))).Returns(repo.Object);

        var sut = new AgentJobsDispatcherService(clusterConfig, factory.Object, new Mock<IJobMasterLogger>().Object);

        var job = new JobRawModel(clusterId)
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            JobDefinitionId = "def",
            NextPlanExecutionAt = DateTime.UtcNow,
            ScheduledAt = DateTime.UtcNow,
            Priority = JobMasterPriority.High,
            Status = JobMasterJobStatus.PendingSave,
            Timeout = TimeSpan.FromSeconds(1),
            MaxNumberOfRetries = 0,
            AgentConnectionId = agentConnId,
            BucketId = bucketId,
            AgentWorkerId = "worker1",
            HostId = new JobMaster.Sdk.Abstractions.Models.Hosts.HostId(clusterId, "testhost"),
        };

        sut.AddSavePendingJob(job);

        repo.Verify(x => x.PushSavePendingJob(It.Is<JobRawModel>(j => j.Id == job.Id)), Times.Once);
        factory.Verify(x => x.GetRepository(It.Is<AgentConnectionId>(a => a.IdValue == agentConnId.IdValue)), Times.Once);
        repo.Verify(x => x.PushSavePendingJob(It.Is<JobRawModel>(j => j.Id == job.Id)), Times.Once);
    }

    [Fact]
    public async Task AddToProcessing_ShouldAssignToBucket_AndPushToRepository()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        clusterConfig.AddAgentConnectionString("agent", "conn", "repo");
        clusterConfig.MarkAsReady();

        var agentConnId = new AgentConnectionId(clusterId, "agent");
        var bucketId = "b1";
        var workerId = "w1";

        var repo = new Mock<IAgentJobsDispatcherRepository>(MockBehavior.Strict);
        repo.SetupGet(x => x.AgentRepoTypeId).Returns("repo");
        repo.SetupGet(x => x.IsPollingBasedForSaving).Returns(true);
        repo.SetupGet(x => x.IsPollingBasedForProcessing).Returns(true);
        repo.Setup(x => x.PushForProcessingAsync(It.IsAny<JobRawModel>()))
            .ReturnsAsync("job-id-2");

        var factory = new Mock<IAgentComponentFactory>(MockBehavior.Strict);
        factory.Setup(x => x.GetRepository(It.Is<AgentConnectionId>(a => a.IdValue == agentConnId.IdValue))).Returns(repo.Object);

        var sut = new AgentJobsDispatcherService(clusterConfig, factory.Object, new Mock<IJobMasterLogger>().Object);

        var job = new JobRawModel(clusterId)
        {
            Id = JobMasterRandomUtil.NewGuid4(),
            JobDefinitionId = "def",
            NextPlanExecutionAt = DateTime.UtcNow,
            ScheduledAt = DateTime.UtcNow,
            Priority = JobMasterPriority.High,
            Status = JobMasterJobStatus.OnMaster,
            Timeout = TimeSpan.FromSeconds(1),
            MaxNumberOfRetries = 0,
        };

        job.AssignToBucket(new BucketModel(clusterId) { Id = bucketId, AgentConnectionId = agentConnId, AgentWorkerId = workerId, HostId = new JobMaster.Sdk.Abstractions.Models.Hosts.HostId(clusterId, "testhost") });

        await sut.AddForProcessingAsync(job);

        job.Status.Should().Be(JobMasterJobStatus.InBucket);
        job.BucketId.Should().Be(bucketId);
        job.AgentWorkerId.Should().Be(workerId);
        job.AgentConnectionId.Should().NotBeNull();
        job.AgentConnectionId!.IdValue.Should().Be(agentConnId.IdValue);

        repo.Verify(x => x.PushForProcessingAsync(It.Is<JobRawModel>(j => j.Id == job.Id && j.BucketId == bucketId)), Times.Once);
        factory.Verify(x => x.GetRepository(It.Is<AgentConnectionId>(a => a.IdValue == agentConnId.IdValue)), Times.Once);
    }

    [Fact]
    public async Task BulkAddSavePendingJobAsync_ShouldPushWholePartitionInOneCall()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);
        clusterConfig.AddAgentConnectionString("agent", "conn", "repo");
        clusterConfig.MarkAsReady();

        var agentConnId = new AgentConnectionId(clusterId, "agent");
        var bucketId = "b1";

        var repo = new Mock<IAgentJobsDispatcherRepository>(MockBehavior.Strict);
        repo.SetupGet(x => x.AgentRepoTypeId).Returns("repo");
        repo.SetupGet(x => x.IsPollingBasedForSaving).Returns(true);
        repo.SetupGet(x => x.IsPollingBasedForProcessing).Returns(true);

        repo.Setup(x => x.BulkPushSavePendingJobAsync(bucketId, It.IsAny<IList<JobRawModel>>()))
            .ReturnsAsync(() => new List<string>(new[] { "job-id-1", "job-id-2" }));

        var factory = new Mock<IAgentComponentFactory>(MockBehavior.Strict);
        factory.Setup(x => x.GetRepository(It.Is<AgentConnectionId>(a => a.IdValue == agentConnId.IdValue))).Returns(repo.Object);

        var sut = new AgentJobsDispatcherService(clusterConfig, factory.Object, new Mock<IJobMasterLogger>().Object);

        // 51 is just "bigger than any typical batch size" -- proves BulkAddSavePendingJobAsync
        // never partitions internally, regardless of what MaxBatchSize a repository type is
        // configured with (partitioning is the caller's responsibility, e.g. JobMasterSchedulerClusterAware).
        var jobs = Enumerable.Range(0, 51)
            .Select(_ => new JobRawModel(clusterId)
            {
                Id = JobMasterRandomUtil.NewGuid4(),
                JobDefinitionId = "def",
                NextPlanExecutionAt = DateTime.UtcNow,
                ScheduledAt = DateTime.UtcNow,
                Priority = JobMasterPriority.High,
                Status = JobMasterJobStatus.OnMaster,
                Timeout = TimeSpan.FromSeconds(1),
                MaxNumberOfRetries = 0,
                AgentConnectionId = agentConnId,
                BucketId = bucketId,
                AgentWorkerId = "worker1",
                HostId = new JobMaster.Sdk.Abstractions.Models.Hosts.HostId(clusterId, "testhost"),
            })
            .ToList();

        await sut.BulkAddSavePendingJobAsync(agentConnId, bucketId, jobs);

        // Partitioning now happens in the caller -- the service pushes whatever it's given in one call.
        repo.Verify(x => x.BulkPushSavePendingJobAsync(bucketId, It.Is<IList<JobRawModel>>(l => l.Count == jobs.Count)), Times.Once);
        factory.Verify(x => x.GetRepository(It.Is<AgentConnectionId>(a => a.IdValue == agentConnId.IdValue)), Times.Once);
    }

    private static string NewClusterId() => $"c{JobMasterRandomUtil.NewGuid4():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);
}
