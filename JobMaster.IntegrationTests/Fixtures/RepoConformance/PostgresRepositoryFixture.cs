using JobMaster.Contracts;
using JobMaster.Contracts.Models;
using JobMaster.Ioc.Extensions;
using JobMaster.Postgres;
using JobMaster.Postgres.Agents;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Models.Agents;
using JobMaster.Sdk.Contracts.Repositories.Agent;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sql;
using JobMaster.IntegrationTests.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Sdk;
using Npgsql;
using Dapper;

namespace JobMaster.IntegrationTests.Fixtures.RepoConformance;

public sealed class PostgresRepositoryFixture : IRepositoryFixture
{
    public string ClusterId { get; } = "ClusterForRepoTests-Postgres-1";

    public AgentConnectionId AgentConnectionId { get; private set; } = null!;

    public IServiceProvider Services { get; private set; } = null!;

    public IMasterJobsRepository MasterJobs { get; private set; } = null!;
    public IMasterRecurringSchedulesRepository MasterRecurringSchedules { get; private set; } = null!;
    public IMasterGenericRecordRepository MasterGenericRecords { get; private set; } = null!;
    public IMasterDistributedLockerRepository MasterDistributedLocker { get; private set; } = null!;

    public IAgentRawMessagesDispatcherRepository AgentMessages { get; private set; } = null!;

    private const string MasterTablePrefix = "JMPostgresTests_";
    private const string AgentTablePrefix = "JMPostgresTests_";

    private const string AgentConnectionName = "AgentForRepoTests-Postgres-1";

    public async Task InitializeAsync()
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        var masterCnn = config["JobMaster:IntegrationTests:MasterPostgres"];
        var agentCnnList = config
            .GetSection("JobMaster:IntegrationTests:AgentsPostgres")
            .Get<string[]>()
            ?? Array.Empty<string>();

        var agentCnn = agentCnnList.FirstOrDefault();

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

        var rawRepo = factory.ClusterServiceProvider.GetRequiredService<PostgresRawMessagesDispatcherRepository>();
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

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
}
