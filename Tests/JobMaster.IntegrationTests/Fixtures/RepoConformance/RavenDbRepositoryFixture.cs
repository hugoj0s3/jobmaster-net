using JobMaster.Abstractions.Models;
using JobMaster.Ioc.Extensions;
using JobMaster.RavenDb;
using JobMaster.RavenDb.Connections;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc;
using Microsoft.Extensions.DependencyInjection;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Testcontainers.RavenDb;

namespace JobMaster.IntegrationTests.Fixtures.RepoConformance;

/// <summary>
/// Standalone RavenDB conformance fixture, deliberately separate from <see cref="RepoConformanceBootstrap"/>
/// and the "RepoConformance" collection the 3 SQL providers share. <see cref="IMasterRecurringSchedulesRepository"/>
/// is the remaining Master interface not implemented yet -- registering a RavenDB cluster into the shared
/// bootstrap would require <c>StartJobMasterRuntimeAsync()</c>, which starts background runners for every
/// registered cluster and would immediately fail against it. This fixture instead builds its own cluster
/// and resolves repositories directly via <see cref="JobMasterClusterAwareComponentFactories.GetFactory"/>
/// -- confirmed safe because that resolution path is plain DI (<c>GetRequiredService</c>) with no
/// dependency on the runtime having been started. Fold this into <see cref="RepoConformanceBootstrap"/>
/// once all 5 Master + Agent interfaces are implemented (see the RavenDB provider plan's roadmap).
/// </summary>
public sealed class RavenDbRepositoryFixture : RepositoryFixtureBase
{
    private const string ClusterIdValue = "RT-RavenDb-1";
    private const string AgentConnectionName = "AgentForRepoTests-RavenDb-1";

    private RavenDbContainer container = null!;
    private ServiceProvider serviceProvider = null!;

    internal override string ClusterId { get; set; } = ClusterIdValue;

    internal override AgentConnectionId AgentConnectionId { get; set; } = null!;

    internal override IMasterJobsRepository MasterJobs { get; set; } = null!;

    internal override IMasterRecurringSchedulesRepository MasterRecurringSchedules
    {
        get => throw new NotImplementedException("RavenDB RecurringSchedules repository -- not implemented until a later increment.");
        set => throw new NotSupportedException();
    }

    internal override IMasterGenericRecordRepository MasterGenericRecords { get; set; } = null!;

    internal override IMasterDistributedLockerRepository MasterDistributedLocker { get; set; } = null!;

    // Not part of RepositoryFixtureBase's contract -- no shared "Logs" RepoConformance category exists
    // for any provider (SQL included), so there's no base-class slot to fill. Exposed as a plain extra
    // property for RavenDbLogsRepositoryConformanceTests, which is RavenDB-only for the same reason.
    internal IMasterLogsRepository MasterLogs { get; set; } = null!;

    // Agent-side fingerprint resolver isn't part of RepositoryFixtureBase's contract either -- this fixture
    // only registers UseRavenDbForMaster, so RegisterForAgent never runs and there's no DI-resolved
    // IAgentFingerprintResolver available. RavenDbAgentFingerprintResolverConformanceTests constructs the
    // resolver directly instead (same document store, no full agent-connection DI wiring needed).
    internal IRavenDbDocumentStoreManager DocumentStoreManager { get; private set; } = null!;
    internal string ConnectionString { get; private set; } = string.Empty;

    internal override IAgentRawMessagesDispatcherRepository AgentMessages { get; set; } = null!;

    public override async Task InitializeAsync()
    {
        // Must track the RavenDB.Client package's major version (7.x) -- an older 5.4 server rejected
        // client 7.2.5's request bodies with a binary/JSON protocol mismatch during manual verification.
        container = new RavenDbBuilder().WithImage("ravendb/ravendb:7.2-ubuntu-latest").Build();
        await container.StartAsync();

        const string database = "RepoConformanceTests";
        var connectionString = $"Urls={container.GetConnectionString()};Database={database}";

        using (var bootstrapStore = new DocumentStore { Urls = [container.GetConnectionString()], Database = database })
        {
            bootstrapStore.Initialize();
            bootstrapStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));
        }

        var services = new ServiceCollection();
        services.AddJobMasterCluster(ClusterIdValue, cfg =>
        {
            cfg.UseRavenDbForMaster(connectionString);
            cfg.AddAgentConnectionConfig(AgentConnectionName)
                .UseRavenDbForAgent(connectionString);
            cfg.Mode(ClusterMode.Active);
        });
        serviceProvider = services.BuildServiceProvider();

        // Deliberately no StartJobMasterRuntimeAsync() call -- see the class doc above.
        var factory = JobMasterClusterAwareComponentFactories.GetFactory(ClusterId);
        MasterDistributedLocker = factory.GetMasterRepository<IMasterDistributedLockerRepository>();
        MasterGenericRecords = factory.GetMasterRepository<IMasterGenericRecordRepository>();
        MasterLogs = factory.GetMasterRepository<IMasterLogsRepository>();
        MasterJobs = factory.GetMasterRepository<IMasterJobsRepository>();

        ConnectionString = connectionString;
        // ClusterServiceProvider, not the fixture's own outer serviceProvider -- ClusterIocRegistration
        // builds its own inner ServiceCollection per cluster (see ClusterIocRegistration.ClusterServices),
        // separate from the container AddJobMasterCluster's caller builds.
        DocumentStoreManager = factory.ClusterServiceProvider.GetRequiredService<IRavenDbDocumentStoreManager>();

        var agentConfig = JobMasterClusterConnectionConfig
            .Get(ClusterId, includeNotReady: true)
            .TryGetAgentConnectionConfig(AgentConnectionName);
        if (agentConfig == null)
        {
            throw new Exception($"Agent config '{AgentConnectionName}' not found for cluster '{ClusterId}'.");
        }

        var rawRepo = factory.ClusterServiceProvider
            .GetRequiredKeyedService<IAgentRawMessagesDispatcherRepository>(
                ClusterServiceKeys.GetAgentRawJobsDispatcherProcessingKey(RavenDbRepositoryConstants.RepositoryTypeId));
        rawRepo.Initialize(agentConfig);
        AgentMessages = rawRepo;

        AgentConnectionId = new AgentConnectionId(ClusterId, AgentConnectionName);
    }

    public override async Task DisposeAsync()
    {
        if (serviceProvider != null)
        {
            await serviceProvider.DisposeAsync();
        }

        if (container != null)
        {
            await container.DisposeAsync();
        }
    }
}
