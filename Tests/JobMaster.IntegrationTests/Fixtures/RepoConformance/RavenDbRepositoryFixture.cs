using JobMaster.Abstractions.Models;
using JobMaster.Ioc.Extensions;
using JobMaster.RavenDb;
using JobMaster.RavenDb.Agents;
using JobMaster.RavenDb.Connections;
using JobMaster.RavenDb.Master;
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
/// and the "RepoConformance" collection the 3 SQL providers share. Registering a RavenDB cluster into that
/// shared bootstrap would require <c>StartJobMasterRuntimeAsync()</c>, which starts background runners for
/// every registered cluster -- this fixture doesn't need those, and avoiding the call also means it must
/// separately mirror the one-time setup steps <c>RavenDbJobMasterRuntimeSetup</c> would otherwise perform
/// (currently: the Message collection's static index, see the <c>RavenDbMessageIndexDefinitions.DeployAsync</c>
/// call below -- database provisioning is still handled manually here too). This fixture instead builds
/// its own cluster and resolves repositories directly via
/// <see cref="JobMasterClusterAwareComponentFactories.GetFactory"/>, which is plain DI
/// (<c>GetRequiredService</c>) with no dependency on the runtime having been started. Fold this into
/// <see cref="RepoConformanceBootstrap"/> once this fixture no longer needs to diverge from a real
/// <c>StartJobMasterRuntimeAsync()</c> startup at all.
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

    internal override IMasterRecurringSchedulesRepository MasterRecurringSchedules { get; set; } = null!;

    internal override IMasterGenericRecordRepository MasterGenericRecords { get; set; } = null!;

    internal override IMasterDistributedLockerRepository MasterDistributedLocker { get; set; } = null!;

    // Not part of RepositoryFixtureBase's contract -- no shared "Logs" RepoConformance category exists
    // for any provider (SQL included), so there's no base-class slot to fill. Exposed as a plain extra
    // property for RavenDbLogsRepositoryConformanceTests, which is RavenDB-only for the same reason.
    internal IMasterLogsRepository MasterLogs { get; set; } = null!;

    // Agent-side fingerprint resolver isn't part of RepositoryFixtureBase's contract either -- this fixture
    // only registers the master side of UseRavenDb, so RegisterForAgent never runs and there's no
    // DI-resolved IAgentFingerprintResolver available. RavenDbAgentFingerprintResolverConformanceTests constructs the
    // resolver directly instead (same document store, no full agent-connection DI wiring needed).
    internal IRavenDbDocumentStoreManager DocumentStoreManager { get; private set; } = null!;
    internal string ConnectionString { get; private set; } = string.Empty;

    internal override IAgentRawMessagesDispatcherRepository AgentMessages { get; set; } = null!;

    public override async Task InitializeAsync()
    {
        // Must track the RavenDB.Client package's major version (7.x) -- a mismatched server major
        // version (e.g. 5.4) rejects this client's request bodies with a binary/JSON protocol mismatch.
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
            cfg.UseRavenDb(connectionString);
            cfg.AddAgentConnectionConfig(AgentConnectionName)
                .UseRavenDb(connectionString);
            cfg.Mode(ClusterMode.Active);
        });
        serviceProvider = services.BuildServiceProvider();

        // Deliberately no StartJobMasterRuntimeAsync() call -- see the class doc above.
        var factory = JobMasterClusterAwareComponentFactories.GetFactory(ClusterId);
        MasterDistributedLocker = factory.GetMasterRepository<IMasterDistributedLockerRepository>();
        MasterGenericRecords = factory.GetMasterRepository<IMasterGenericRecordRepository>();
        MasterLogs = factory.GetMasterRepository<IMasterLogsRepository>();
        MasterJobs = factory.GetMasterRepository<IMasterJobsRepository>();
        MasterRecurringSchedules = factory.GetMasterRepository<IMasterRecurringSchedulesRepository>();

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

        // Normally deployed by RavenDbJobMasterRuntimeSetup.OnBeforeStartAsync, which this fixture
        // deliberately never invokes (see the class doc above) -- without it, RavenDbRawMessagesDispatcherRepository's
        // queries (which target this index by name) throw IndexDoesNotExistException.
        var messageCollectionName = agentConfig.GetCollectionPrefix() + RavenDbRawMessagesDispatcherRepository.Collection;
        await RavenDbMessageIndexDefinitions.DeployAsync(DocumentStoreManager.GetOrCreateStore(connectionString), messageCollectionName);

        // Same reasoning, for AcquireAndFetchAsync's static index -- lives in the master database.
        var clusterConfig = JobMasterClusterConnectionConfig.Get(ClusterId, includeNotReady: true);
        var jobCollectionName = clusterConfig.GetCollectionPrefix() + RavenDbMasterJobsRepository.Collection;
        await RavenDbJobIndexDefinitions.DeployAsync(DocumentStoreManager.GetOrCreateStore(connectionString), jobCollectionName);

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
