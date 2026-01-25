using JobMaster.IntegrationTests.Utils;
using JobMaster.Ioc.Extensions;
using JobMaster.SqlBase;
using JobMaster.SqlServer;
using JobMaster.SqlServer.Agents;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Sdk;
using Microsoft.Data.SqlClient;
using Dapper;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;

namespace JobMaster.IntegrationTests.Fixtures.RepoConformance;

public sealed class SqlServerRepositoryFixture : RepositoryFixtureBase
{
    internal override string ClusterId { get; set; } = "ClusterForRepoTests-SqlServer-1";

    internal override AgentConnectionId AgentConnectionId { get; set; } = null!;

    internal override IServiceProvider Services { get; set; } = null!;

    internal override IMasterJobsRepository MasterJobs { get; set; } = null!;
    internal override IMasterRecurringSchedulesRepository MasterRecurringSchedules { get; set; } = null!;
    internal override IMasterGenericRecordRepository MasterGenericRecords { get; set; } = null!;
    internal override IMasterDistributedLockerRepository MasterDistributedLocker { get; set; } = null!;

    internal override IAgentRawMessagesDispatcherRepository AgentMessages { get;  set; } = null!;

    private const string MasterTablePrefix = "JMSqlServerTests_";
    private const string AgentTablePrefix = "JMSqlServerTests_";

    private const string AgentConnectionName = "AgentForConformanceRepoTests-SqlServer-1";

    public override async Task InitializeAsync()
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        var masterCnn = config["JobMaster:IntegrationTests:MasterSqlServer"];
        var agentCnnList = config
            .GetSection("JobMaster:IntegrationTests:AgentsSqlServer")
            .Get<string[]>()
            ?? Array.Empty<string>();

        var agentCnn = agentCnnList.FirstOrDefault();

        if (string.IsNullOrWhiteSpace(masterCnn) || string.IsNullOrWhiteSpace(agentCnn))
        {
            Services = new ServiceCollection().BuildServiceProvider();
            throw new SkipException("Repo conformance tests require JobMaster:IntegrationTests:MasterSqlServer and at least one entry in AgentsSqlServer.");
        }

        // Ensure databases exist before running tests (create on the fly if missing)
        await EnsureSqlServerDatabaseExistsAsync(masterCnn);
        await EnsureSqlServerDatabaseExistsAsync(agentCnn);

        await SqlServerTestDbUtil.DropJobMasterTablesAsync(
            masterCnn,
            MasterTablePrefix,
            agentCnn,
            AgentTablePrefix);

        var services = new ServiceCollection();

        services.AddJobMasterCluster(ClusterId, cfg =>
        {
            cfg.UseSqlServerForMaster(masterCnn);
            cfg.UseSqlTablePrefixForMaster(MasterTablePrefix);

            cfg.AddAgentConnectionConfig(AgentConnectionName)
                .UseSqlServerForAgent(agentCnn)
                .UseSqlTablePrefixForAgent(AgentTablePrefix);

            cfg.ClusterMode(ClusterMode.Active);
        });

        Services = services.BuildServiceProvider();

        await Services.StartJobMasterRuntimeAsync();

        var factory = JobMasterClusterAwareComponentFactories.GetFactory(ClusterId);

        MasterJobs = factory.GetMasterRepository<IMasterJobsRepository>();
        MasterRecurringSchedules = factory.GetMasterRepository<IMasterRecurringSchedulesRepository>();
        MasterGenericRecords = factory.GetMasterRepository<IMasterGenericRecordRepository>();
        MasterDistributedLocker = factory.GetMasterRepository<IMasterDistributedLockerRepository>();

        var agentConfig = JobMasterClusterConnectionConfig
            .Get(ClusterId, includeInactive: true)
            .TryGetAgentConnectionConfig(AgentConnectionName);

        if (agentConfig == null)
        {
            throw new Exception($"Agent config '{AgentConnectionName}' not found for cluster '{ClusterId}'.");
        }

        var rawRepo = factory.ClusterServiceProvider.GetRequiredService<SqlServerRawMessagesDispatcherRepository>();
        rawRepo.Initialize(agentConfig);
        AgentMessages = rawRepo;

        AgentConnectionId = new AgentConnectionId(ClusterId, AgentConnectionName);
    }

    private static async Task EnsureSqlServerDatabaseExistsAsync(string connectionString)
    {
        var csb = new SqlConnectionStringBuilder(connectionString);
        var databaseName = csb.InitialCatalog;
        if (string.IsNullOrWhiteSpace(databaseName)) return;

        // Connect to master to create DB if needed
        var masterCsb = new SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = "master"
        };

        await using var conn = new SqlConnection(masterCsb.ConnectionString);
        await conn.OpenAsync();
        var exists = await conn.ExecuteScalarAsync<int>("SELECT CASE WHEN DB_ID(@db) IS NULL THEN 0 ELSE 1 END", new { db = databaseName });
        if (exists == 0)
        {
            // Note: database name is parameterized only for existence check; CREATE DATABASE cannot parameterize identifiers.
            await conn.ExecuteAsync($"CREATE DATABASE [{databaseName}];");
        }
    }

    public override Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
}
