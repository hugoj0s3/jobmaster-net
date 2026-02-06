using JobMaster.IntegrationTests.Utils;
using JobMaster.Ioc.Extensions;
using JobMaster.MySql;
using JobMaster.SqlBase;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Sdk;
using Dapper;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc;
using MySqlConnector;

namespace JobMaster.IntegrationTests.Fixtures.RepoConformance;

public sealed class MySqlRepositoryFixture : RepositoryFixtureBase
{
    internal override string ClusterId { get; set; } = "ClusterForRepoTests-MySql-1";

    internal override AgentConnectionId AgentConnectionId { get; set; } = null!;

    internal override IServiceProvider Services { get; set; } = null!;

    internal override IMasterJobsRepository MasterJobs { get;  set; } = null!;
    internal override IMasterRecurringSchedulesRepository MasterRecurringSchedules { get; set; } = null!;
    internal override IMasterGenericRecordRepository MasterGenericRecords { get;  set; } = null!;
    internal override IMasterDistributedLockerRepository MasterDistributedLocker { get; set; } = null!;

    internal override IAgentRawMessagesDispatcherRepository AgentMessages { get; set; } = null!;

    private const string MasterTablePrefix = "JMMySqlTests_";
    private const string AgentTablePrefix = "JMMySqlTests_";

    private const string AgentConnectionName = "AgentForRepoTests-MySql-1";

    public override async Task InitializeAsync()
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true)
            .AddUserSecrets(typeof(MySqlRepositoryFixture).Assembly, optional: true)
            .AddEnvironmentVariables()
            .Build();

        var masterCnn = config["JobMaster:IntegrationTests:MasterMySql"];
        var agentCnnList = config
            .GetSection("JobMaster:IntegrationTests:AgentsMySql")
            .Get<string[]>()
            ?? Array.Empty<string>();

        var agentCnn = agentCnnList.FirstOrDefault();

        masterCnn = IntegrationTestSecrets.ApplySecrets(masterCnn, "MySql", config);
        agentCnn = IntegrationTestSecrets.ApplySecrets(agentCnn, "MySql", config);

        if (string.IsNullOrWhiteSpace(masterCnn) || string.IsNullOrWhiteSpace(agentCnn))
        {
            Services = new ServiceCollection().BuildServiceProvider();
            throw new SkipException("Repo conformance tests require JobMaster:IntegrationTests:MasterMySql and at least one entry in AgentsMySql.");
        }

        // Ensure databases exist before running tests (create on the fly if missing)
        await EnsureMySqlDatabaseExistsAsync(masterCnn);
        await EnsureMySqlDatabaseExistsAsync(agentCnn);

        await MySqlTestDbUtil.DropJobMasterTablesAsync(
            masterCnn,
            MasterTablePrefix,
            agentCnn,
            AgentTablePrefix);

        var services = new ServiceCollection();

        services.AddJobMasterCluster(ClusterId, cfg =>
        {
            cfg.UseMySqlForMaster(masterCnn);
            cfg.UseSqlTablePrefixForMaster(MasterTablePrefix);

            cfg.AddAgentConnectionConfig(AgentConnectionName)
                .UseMySqlForAgent(agentCnn)
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

        var rawRepo = factory.ClusterServiceProvider
            .GetRequiredKeyedService<IAgentRawMessagesDispatcherRepository>(
                ClusterServiceKeys.GetAgentRawJobsDispatcherProcessingKey(MySqlRepositoryConstants.RepositoryTypeId)
            );
        rawRepo.Initialize(agentConfig);
        AgentMessages = rawRepo;

        AgentConnectionId = new AgentConnectionId(ClusterId, AgentConnectionName);
    }

    private static async Task EnsureMySqlDatabaseExistsAsync(string connectionString)
    {
        var builder = new MySqlConnectionStringBuilder(connectionString);
        var dbName = builder.Database;
        if (string.IsNullOrWhiteSpace(dbName)) return;

        // MySQL requires connecting to a DB; use 'mysql' schema
        var adminBuilder = new MySqlConnectionStringBuilder(connectionString)
        {
            Database = "mysql"
        };

        await using var conn = new MySqlConnection(adminBuilder.ConnectionString);
        await conn.OpenAsync();
        var exists = await conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = @db", new { db = dbName });
        if (exists == 0)
        {
            await conn.ExecuteAsync($"CREATE DATABASE `{dbName}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;");
        }
    }

    public override Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
}
