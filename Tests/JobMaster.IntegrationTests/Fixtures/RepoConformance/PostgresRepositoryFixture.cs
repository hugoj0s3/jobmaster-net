using Microsoft.Extensions.DependencyInjection;
using JobMaster.Postgres;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc;

namespace JobMaster.IntegrationTests.Fixtures.RepoConformance;

public sealed class PostgresRepositoryFixture : RepositoryFixtureBase
{
    internal override string ClusterId { get; set; } = RepoConformanceBootstrap.PostgresClusterId;

    internal override AgentConnectionId AgentConnectionId { get; set; } = null!;

    internal override IMasterJobsRepository MasterJobs { get; set; } = null!;
    internal override IMasterRecurringSchedulesRepository MasterRecurringSchedules { get; set; } = null!;
    internal override IMasterGenericRecordRepository MasterGenericRecords { get; set; } = null!;
    internal override IMasterDistributedLockerRepository MasterDistributedLocker { get; set; } = null!;
    internal override IAgentRawMessagesDispatcherRepository AgentMessages { get; set; } = null!;

    public override async Task InitializeAsync()
    {
        // The shared runtime (all 3 providers' clusters, started once) is owned by
        // RepoConformanceBootstrap -- see its class doc for why this can't be done per-fixture.
        var bootstrap = await RepoConformanceBootstrap.GetInstanceAsync();
        await bootstrap.EnsureStartedAsync();

        var factory = JobMasterClusterAwareComponentFactories.GetFactory(ClusterId);

        MasterJobs = factory.GetMasterRepository<IMasterJobsRepository>();
        MasterRecurringSchedules = factory.GetMasterRepository<IMasterRecurringSchedulesRepository>();
        MasterGenericRecords = factory.GetMasterRepository<IMasterGenericRecordRepository>();
        MasterDistributedLocker = factory.GetMasterRepository<IMasterDistributedLockerRepository>();

        var agentConfig = JobMasterClusterConnectionConfig
            .Get(ClusterId, includeNotReady: true)
            .TryGetAgentConnectionConfig(RepoConformanceBootstrap.PostgresAgentConnectionName);

        if (agentConfig == null)
        {
            throw new Exception($"Agent config '{RepoConformanceBootstrap.PostgresAgentConnectionName}' not found for cluster '{ClusterId}'.");
        }

        var rawRepo = factory.ClusterServiceProvider
            .GetRequiredKeyedService<IAgentRawMessagesDispatcherRepository>(
                ClusterServiceKeys.GetAgentRawJobsDispatcherProcessingKey(PostgresRepositoryConstants.RepositoryTypeId)
            );
        rawRepo.Initialize(agentConfig);
        AgentMessages = rawRepo;

        AgentConnectionId = new AgentConnectionId(ClusterId, RepoConformanceBootstrap.PostgresAgentConnectionName);
    }
}
