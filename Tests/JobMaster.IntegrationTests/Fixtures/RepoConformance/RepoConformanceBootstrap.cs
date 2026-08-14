using System.Security.Cryptography;
using JobMaster.Abstractions.Models;
using JobMaster.Ioc.Extensions;
using JobMaster.MySql;
using JobMaster.Postgres;
using JobMaster.Postgres.Agents;
using JobMaster.SqlBase;
using JobMaster.SqlServer;
using JobMaster.SqlServer.Agents;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.MsSql;
using Testcontainers.MySql;
using Testcontainers.PostgreSql;
using Xunit;

namespace JobMaster.IntegrationTests.Fixtures.RepoConformance;

/// <summary>
/// <c>JobMasterRuntime</c> is a genuine process-wide singleton (<c>JobMasterRuntimeSingleton.Instance</c>)
/// -- <c>StartJobMasterRuntimeAsync()</c> is only ever meant to be called once per process, after
/// every cluster it will manage has already been registered on the same <see cref="IServiceProvider"/>.
/// The three provider fixtures (<see cref="PostgresRepositoryFixture"/>, <see cref="MySqlRepositoryFixture"/>,
/// <see cref="SqlServerRepositoryFixture"/>) each need their own cluster, so this shared bootstrap --
/// one instance per test run, registered as a collection fixture alongside them -- owns all three
/// Testcontainers databases and the single <see cref="ServiceCollection"/> + start call all three
/// fixtures rely on. Calling <c>StartJobMasterRuntimeAsync()</c> independently per fixture throws
/// "JobMasterRuntime is already started" for every fixture after the first.
///
/// xunit v2 collection fixtures can't take other collection fixtures as constructor parameters (only
/// class fixtures can depend on collection fixtures, not fixture-to-fixture), so the 3 provider
/// fixtures can't have this injected directly even though all 4 are registered on the same
/// <c>RepoConformanceCollection</c>. <see cref="GetInstanceAsync"/> is the workaround: xunit
/// constructs every declared <c>ICollectionFixture&lt;T&gt;</c> for a collection before calling
/// <c>InitializeAsync</c> on any of them, so by the time a provider fixture's own
/// <c>InitializeAsync</c> runs, this constructor has already resolved the shared instance.
/// </summary>
public sealed class RepoConformanceBootstrap : IAsyncLifetime
{
    public const string PostgresClusterId = "RT-Postgres-1";
    public const string PostgresTablePrefix = "JMPostgresTests_";
    public const string PostgresAgentConnectionName = "AgentForRepoTests-Postgres-1";

    public const string MySqlClusterId = "RT-MySql-1";
    public const string MySqlTablePrefix = "JMMyT_";
    public const string MySqlAgentConnectionName = "Agent-MySql-1";

    public const string SqlServerClusterId = "RT-SqlServer-1";
    public const string SqlServerTablePrefix = "JMSqlServerTests_";
    public const string SqlServerAgentConnectionName = "Agent-SqlServer-1";

    private static readonly TaskCompletionSource<RepoConformanceBootstrap> InstanceSource =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public static Task<RepoConformanceBootstrap> GetInstanceAsync() => InstanceSource.Task;

    private readonly SemaphoreSlim startLock = new(1, 1);
    private bool started;

    private PostgreSqlContainer postgres = null!;
    private MySqlContainer mySql = null!;
    private MsSqlContainer sqlServer = null!;
    private ServiceProvider serviceProvider = null!;

    public RepoConformanceBootstrap()
    {
        InstanceSource.TrySetResult(this);
    }

    public Task InitializeAsync() => Task.CompletedTask;

    /// <summary>
    /// Starts all three databases and the shared JobMaster runtime, exactly once regardless of how
    /// many of the three provider fixtures call this concurrently during collection-fixture setup.
    /// </summary>
    public async Task EnsureStartedAsync()
    {
        if (started) return;

        await startLock.WaitAsync();
        try
        {
            if (started) return;

            postgres = new PostgreSqlBuilder()
                .WithImage("postgres:16-alpine")
                .WithDatabase("postgres")
                .WithUsername("postgres")
                .WithPassword(GenerateContainerPassword())
                .Build();

            mySql = new MySqlBuilder()
                .WithImage("mysql:8.0")
                .WithDatabase("mysql")
                .WithUsername("root")
                .WithPassword(GenerateContainerPassword())
                .Build();

            sqlServer = new MsSqlBuilder()
                .WithPassword(GenerateContainerPassword())
                .Build();

            await Task.WhenAll(postgres.StartAsync(), mySql.StartAsync(), sqlServer.StartAsync());
            var sqlServerConnectionString = await CreateSqlServerDatabaseWithRcsiAsync(sqlServer.GetConnectionString(), "RepoConformanceTests");

            var services = new ServiceCollection();

            services.AddJobMasterCluster(PostgresClusterId, cfg =>
            {
                cfg.UsePostgresForMaster(postgres.GetConnectionString());
                cfg.UseSqlTablePrefixForMaster(PostgresTablePrefix);

                cfg.AddAgentConnectionConfig(PostgresAgentConnectionName)
                    .UsePostgresForAgent(postgres.GetConnectionString())
                    .UseSqlTablePrefixForAgent(PostgresTablePrefix);

                cfg.Mode(ClusterMode.Active);
                // Registering 3 clusters on one ServiceCollection requires exactly one default --
                // PreValidation throws "Multiple clusters configured but no default cluster defined"
                // otherwise. Arbitrary choice among the 3; nothing here relies on which one it is.
                cfg.SetAsDefault();
            });

            services.AddJobMasterCluster(MySqlClusterId, cfg =>
            {
                cfg.UseMySqlForMaster(mySql.GetConnectionString());
                cfg.UseSqlTablePrefixForMaster(MySqlTablePrefix);

                cfg.AddAgentConnectionConfig(MySqlAgentConnectionName)
                    .UseMySqlForAgent(mySql.GetConnectionString())
                    .UseSqlTablePrefixForAgent(MySqlTablePrefix);

                cfg.Mode(ClusterMode.Active);
            });

            services.AddJobMasterCluster(SqlServerClusterId, cfg =>
            {
                cfg.UseSqlServerForMaster(sqlServerConnectionString);
                cfg.UseSqlTablePrefixForMaster(SqlServerTablePrefix);

                cfg.AddAgentConnectionConfig(SqlServerAgentConnectionName)
                    .UseSqlServerForAgent(sqlServerConnectionString)
                    .UseSqlTablePrefixForAgent(SqlServerTablePrefix);

                cfg.Mode(ClusterMode.Active);
            });

            serviceProvider = services.BuildServiceProvider();
            await serviceProvider.StartJobMasterRuntimeAsync();

            started = true;
        }
        finally
        {
            startLock.Release();
        }
    }

    public async Task DisposeAsync()
    {
        if (serviceProvider != null) await serviceProvider.DisposeAsync();
        if (postgres != null) await postgres.DisposeAsync();
        if (mySql != null) await mySql.DisposeAsync();
        if (sqlServer != null) await sqlServer.DisposeAsync();
    }

    private static string GenerateContainerPassword(int byteLength = 18)
    {
        var bytes = RandomNumberGenerator.GetBytes(byteLength);
        return Convert.ToBase64String(bytes).Replace("+", "-").Replace("/", "_").TrimEnd('=');
    }

    // JobMaster relies on non-blocking reads for counts/queries/heartbeats (see JobMaster.SqlServer's
    // README) -- RCSI gives READ COMMITTED readers a row-versioned snapshot instead of shared locks,
    // so readers never block writers and vice versa. READ_COMMITTED_SNAPSHOT can't be set on the
    // system "master" database, so this creates a dedicated one first -- same two-step
    // CREATE-then-ALTER this repo's JobMaster.ScenarioTests already uses (SqlServerDatabaseProvisioner).
    private static async Task<string> CreateSqlServerDatabaseWithRcsiAsync(string connectionString, string databaseName)
    {
        await using (var conn = new SqlConnection(connectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"CREATE DATABASE [{databaseName}];";
            await cmd.ExecuteNonQueryAsync();

            await using var rcsiCmd = conn.CreateCommand();
            rcsiCmd.CommandText = $"ALTER DATABASE [{databaseName}] SET READ_COMMITTED_SNAPSHOT ON;";
            await rcsiCmd.ExecuteNonQueryAsync();
        }

        var builder = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = databaseName };
        return builder.ConnectionString;
    }
}
