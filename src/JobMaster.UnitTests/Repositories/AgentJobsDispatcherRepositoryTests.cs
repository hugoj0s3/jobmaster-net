using System.Reflection;
using FluentAssertions;
using Moq;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Models;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Repositories.Agent;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Repositories;

namespace JobMaster.UnitTests.Repositories;

public class AgentJobsDispatcherRepositoryTests
{
    [Fact]
    public void EnforceDispatchSizeLimit_WhenEstimatedSizeExceedsMax_ShouldThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var masterConfigSvc = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        masterConfigSvc.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId)
        {
            MaxMessageByteSize = 1
        });

        var repo = new TestAgentJobsDispatcherRepository(
            clusterConfig,
            masterConfigSvc.Object,
            new Mock<IJobMasterLogger>().Object,
            new FakeRawRepo(clusterConfig),
            new FakeRawRepo(clusterConfig));

        var method = typeof(AgentJobsDispatcherRepository<FakeRawRepo, FakeRawRepo>)
            .GetMethod("EnforceDispatchSizeLimit", BindingFlags.Instance | BindingFlags.NonPublic);

        method.Should().NotBeNull();

        var payload = "{}";
        var correlationId = new string('a', 32);

        var act = () => method!.Invoke(repo, new object[] { payload, correlationId });

        act.Should().Throw<TargetInvocationException>()
            .WithInnerException<ArgumentException>();

        masterConfigSvc.VerifyAll();
    }

    [Fact]
    public void EnforceDispatchSizeLimit_WhenEstimatedSizeWithinMax_ShouldNotThrow()
    {
        var clusterId = NewClusterId();
        var clusterConfig = CreateClusterConfig(clusterId);

        var masterConfigSvc = new Mock<IMasterClusterConfigurationService>(MockBehavior.Strict);
        masterConfigSvc.Setup(x => x.Get()).Returns(new ClusterConfigurationModel(clusterId)
        {
            MaxMessageByteSize = 1024 * 1024
        });

        var repo = new TestAgentJobsDispatcherRepository(
            clusterConfig,
            masterConfigSvc.Object,
            new Mock<IJobMasterLogger>().Object,
            new FakeRawRepo(clusterConfig),
            new FakeRawRepo(clusterConfig));

        var method = typeof(AgentJobsDispatcherRepository<FakeRawRepo, FakeRawRepo>)
            .GetMethod("EnforceDispatchSizeLimit", BindingFlags.Instance | BindingFlags.NonPublic);

        method.Should().NotBeNull();

        var payload = "{}";
        var correlationId = new string('a', 32);

        var act = () => method!.Invoke(repo, new object[] { payload, correlationId });

        act.Should().NotThrow();

        masterConfigSvc.VerifyAll();
    }

    private sealed class TestAgentJobsDispatcherRepository : AgentJobsDispatcherRepository<FakeRawRepo, FakeRawRepo>
    {
        public TestAgentJobsDispatcherRepository(
            JobMasterClusterConnectionConfig clusterConnConfig,
            IMasterClusterConfigurationService masterClusterConfigurationService,
            IJobMasterLogger logger,
            FakeRawRepo savePendingRepository,
            FakeRawRepo processingRepository)
            : base(clusterConnConfig, masterClusterConfigurationService, savePendingRepository, processingRepository, logger)
        {
        }

        public override string AgentRepoTypeId => "test";
    }

    private sealed class FakeRawRepo : IAgentRawMessagesDispatcherRepository
    {
        public FakeRawRepo(JobMasterClusterConnectionConfig clusterConnConfig)
        {
            ClusterConnConfig = clusterConnConfig;
        }

        public JobMasterClusterConnectionConfig ClusterConnConfig { get; }

        public string PushMessage(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId) => throw new NotImplementedException();
        public Task<string> PushMessageAsync(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId) => throw new NotImplementedException();
        public Task<IList<string>> BulkPushMessageAsync(string fullBucketAddressId, IList<(string payload, DateTime referenceTime, string correlationId)> messages) => throw new NotImplementedException();
        public Task<IList<JobMasterRawMessage>> DequeueMessagesAsync(string fullBucketAddressId, int numberOfJobs, DateTime? referenceTimeTo = null) => Task.FromResult<IList<JobMasterRawMessage>>(new List<JobMasterRawMessage>());
        public bool HasJobs(string fullBucketAddressId) => false;
        public Task<bool> HasJobsAsync(string fullBucketAddressId) => Task.FromResult(false);

        public void CreateBucket(string fullBucketAddressId) { }
        public void DestroyBucket(string fullBucketAddressId) { }
        public Task CreateBucketAsync(string fullBucketAddressId)
        {
            CreateBucket(fullBucketAddressId);
            return Task.CompletedTask;
        }

        public Task DestroyBucketAsync(string fullBucketAddressId)
        {
            DestroyBucket(fullBucketAddressId);
            return Task.CompletedTask;
        }

        public void Initialize(JobMasterAgentConnectionConfig config) { }
        public bool IsAutoDequeue => false;
        public string AgentRepoTypeId => "test";
    }

    private static string NewClusterId() => $"c{Guid.NewGuid():N}";

    private static JobMasterClusterConnectionConfig CreateClusterConfig(string clusterId)
        => JobMasterClusterConnectionConfig.Create(clusterId, "repo", "conn", isDefault: true);
}
