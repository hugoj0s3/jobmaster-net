using JobMaster.Abstractions.Models;
using JobMaster.Ioc.Extensions;
using JobMaster.RavenDb;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using Microsoft.Extensions.DependencyInjection;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Testcontainers.RavenDb;

namespace JobMaster.IntegrationTests.Fixtures.RepoConformance;

/// <summary>
/// Standalone RavenDB conformance fixture, deliberately separate from <see cref="RepoConformanceBootstrap"/>
/// and the "RepoConformance" collection the 3 SQL providers share. Only <see cref="IMasterDistributedLockerRepository"/>
/// and <see cref="IMasterGenericRecordRepository"/> exist so far -- registering a RavenDB cluster into
/// the shared bootstrap would require
/// <c>StartJobMasterRuntimeAsync()</c>, which starts background runners for every registered cluster and
/// would immediately fail against the 4 Master interfaces that don't exist yet. This fixture instead
/// builds its own cluster and resolves the lock repository directly via
/// <see cref="JobMasterClusterAwareComponentFactories.GetFactory"/> -- confirmed safe because that
/// resolution path is plain DI (<c>GetRequiredService</c>) with no dependency on the runtime having been
/// started. Fold this into <see cref="RepoConformanceBootstrap"/> once all 5 Master + Agent interfaces
/// are implemented (see the RavenDB provider plan's roadmap).
/// </summary>
public sealed class RavenDbRepositoryFixture : RepositoryFixtureBase
{
    private const string ClusterIdValue = "RT-RavenDb-1";

    private RavenDbContainer container = null!;
    private ServiceProvider serviceProvider = null!;

    internal override string ClusterId { get; set; } = ClusterIdValue;

    internal override AgentConnectionId AgentConnectionId { get; set; } = null!;

    internal override IMasterJobsRepository MasterJobs
    {
        get => throw new NotImplementedException("RavenDB Jobs repository -- not implemented until a later increment.");
        set => throw new NotSupportedException();
    }

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

    internal override IAgentRawMessagesDispatcherRepository AgentMessages
    {
        get => throw new NotImplementedException("RavenDB agent messages repository -- not implemented until a later increment.");
        set => throw new NotSupportedException();
    }

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
            cfg.Mode(ClusterMode.Active);
        });
        serviceProvider = services.BuildServiceProvider();

        // Deliberately no StartJobMasterRuntimeAsync() call -- see the class doc above.
        var factory = JobMasterClusterAwareComponentFactories.GetFactory(ClusterId);
        MasterDistributedLocker = factory.GetMasterRepository<IMasterDistributedLockerRepository>();
        MasterGenericRecords = factory.GetMasterRepository<IMasterGenericRecordRepository>();
        MasterLogs = factory.GetMasterRepository<IMasterLogsRepository>();
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
