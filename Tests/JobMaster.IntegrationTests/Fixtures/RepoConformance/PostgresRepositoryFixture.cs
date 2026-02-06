using JobMaster.Ioc.Extensions;
using JobMaster.Postgres;
using JobMaster.Postgres.Agents;
using JobMaster.SqlBase;
using JobMaster.IntegrationTests.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Sdk;
using Npgsql;
using Dapper;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc;

namespace JobMaster.IntegrationTests.Fixtures.RepoConformance;

public sealed class PostgresRepositoryFixture : RepositoryFixtureBase
{
    internal override string ClusterId { get; set; } = "ClusterForRepoTests-Postgres-1";

    internal override AgentConnectionId AgentConnectionId { get; set; } = null!;

    internal override IServiceProvider Services { get; set; } = null!;

    internal override IMasterJobsRepository MasterJobs { get; set; } = null!;
    internal override IMasterRecurringSchedulesRepository MasterRecurringSchedules { get; set; } = null!;
    internal override IMasterGenericRecordRepository MasterGenericRecords { get; set; } = null!;
    internal override IMasterDistributedLockerRepository MasterDistributedLocker { get; set; } = null!;

    internal override IAgentRawMessagesDispatcherRepository AgentMessages { get;  set; } = null!;

    private const string MasterTablePrefix = "JMPostgresTests_";
    private const string AgentTablePrefix = "JMPostgresTests_";

    private const string AgentConnectionName = "AgentForRepoTests-Postgres-1";

    public override async Task InitializeAsync()
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true)
            .AddUserSecrets(typeof(PostgresRepositoryFixture).Assembly, optional: true)
            .AddEnvironmentVariables()
            .Build();

        var masterCnn = config["JobMaster:IntegrationTests:MasterPostgres"];
        var agentCnnList = config
            .GetSection("JobMaster:IntegrationTests:AgentsPostgres")
            .Get<string[]>()
            ?? Array.Empty<string>();

        var agentCnn = agentCnnList.FirstOrDefault();

        masterCnn = IntegrationTestSecrets.ApplySecrets(masterCnn, "Postgres", config);
        agentCnn = IntegrationTestSecrets.ApplySecrets(agentCnn, "Postgres", config);

        if (string.IsNullOrWhiteSpace(masterCnn) || string.IsNullOrWhiteSpace(agentCnn))
        {
            Services = new ServiceCollection().BuildServiceProvider();
            throw new SkipException("Repo conformance tests require JobMaster:IntegrationTests:MasterPostgres and at least one entry in AgentsPostgres.");
        }

        // Ensure databases exist before running tests (create on the fly if missing)
        await EnsurePostgresDatabaseExistsAsync(masterCnn);
        await EnsurePostgresDatabaseExistsAsync(agentCnn);

        await PostgresTestDbUtil.DropJobMasterTablesAsync(
            masterCnn,
            MasterTablePrefix,
            agentCnn,
            AgentTablePrefix);

        var services = new ServiceCollection();

        services.AddJobMasterCluster(ClusterId, cfg =>
        {
            cfg.UsePostgresForMaster(masterCnn);
            cfg.UseSqlTablePrefixForMaster(MasterTablePrefix);

            cfg.AddAgentConnectionConfig(AgentConnectionName)
                .UsePostgresForAgent(agentCnn)
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
                ClusterServiceKeys.GetAgentRawJobsDispatcherProcessingKey(PostgresRepositoryConstants.RepositoryTypeId)
            );
        rawRepo.Initialize(agentConfig);
        AgentMessages = rawRepo;

        AgentConnectionId = new AgentConnectionId(ClusterId, AgentConnectionName);
    }

    private static async Task EnsurePostgresDatabaseExistsAsync(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var dbName = builder.Database;
        if (string.IsNullOrWhiteSpace(dbName)) return;

        // Connect to default postgres database to create target DB if needed
        var adminBuilder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Database = "postgres"
        };

        await using var conn = new NpgsqlConnection(adminBuilder.ConnectionString);
        await conn.OpenAsync();
        var exists = await conn.ExecuteScalarAsync<int>("SELECT 1 FROM pg_database WHERE datname = @db", new { db = dbName });
        if (exists != 1)
        {
            await conn.ExecuteAsync($"CREATE DATABASE \"{dbName}\";");
        }
    }

    public override Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
}
