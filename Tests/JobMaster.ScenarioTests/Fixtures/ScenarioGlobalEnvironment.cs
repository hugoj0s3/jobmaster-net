using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Images;
using DotNet.Testcontainers.Networks;
using JobMaster.ScenarioTests;
using JobMaster.ScenarioTests.Runner;
using Testcontainers.MsSql;
using Testcontainers.MySql;
using Testcontainers.Nats;
using Testcontainers.PostgreSql;
using Testcontainers.RavenDb;
using Testcontainers.Redis;
using Xunit;

namespace JobMaster.ScenarioTests.Fixtures;

/// <summary>
/// Shared, run-scoped infrastructure: one Postgres/MySql/SqlServer/Nats and one Redis container
/// for the entire test run (each lazily started on first use), plus every distinct scenario app
/// image (keyed by Dockerfile path -- see <see cref="GetOrBuildAppImageAsync"/>) built once and
/// reused by every scenario. Disposed only when the whole test run ends.
/// </summary>
public sealed class ScenarioGlobalEnvironment : IAsyncLifetime
{
    public const string PostgresUsername = "postgres";
    public const string MySqlUsername = "root";
    public const string SqlServerUsername = "sa";
    public const string NatsUsername = "natsuser";

    /// <summary>Generated once per test run — never hardcoded, never logged.</summary>
    public string PostgresPassword { get; } = SecretGenerator.Generate();

    /// <summary>Generated once per test run — never hardcoded, never logged.</summary>
    public string MySqlPassword { get; } = SecretGenerator.Generate();

    /// <summary>Generated once per test run — never hardcoded, never logged.</summary>
    public string SqlServerPassword { get; } = SecretGenerator.Generate();

    /// <summary>Generated once per test run — never hardcoded, never logged.</summary>
    public string NatsPassword { get; } = SecretGenerator.Generate();

    private readonly SemaphoreSlim postgresLock = new(1, 1);
    private readonly SemaphoreSlim mySqlLock = new(1, 1);
    private readonly SemaphoreSlim sqlServerLock = new(1, 1);
    private readonly SemaphoreSlim redisLock = new(1, 1);
    private readonly SemaphoreSlim natsLock = new(1, 1);
    private readonly SemaphoreSlim ravenDbLock = new(1, 1);
    private readonly SemaphoreSlim imageBuildLock = new(1, 1);

    private PostgreSqlContainer? postgres;
    private MySqlContainer? mySql;
    private MsSqlContainer? sqlServer;
    private RedisContainer? redis;
    private NatsContainer? nats;
    private RavenDbContainer? ravenDb;

    // Keyed by (normalized) DockerfilePath, so any number of distinct scenario app images can be
    // built and cached independently within one run -- not just the original schedule-app/api-app
    // pair. See ScenarioRunner.StartContainerAsync, which now drives image selection entirely off
    // ContainerDefinition.DockerfilePath instead of a hardcoded schedule-app-vs-api-app branch.
    private readonly Dictionary<string, IFutureDockerImage> images = new(StringComparer.OrdinalIgnoreCase);

    public INetwork Network { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        Network = new NetworkBuilder().Build();
        await Network.CreateAsync();
    }

    public async Task<PostgreSqlContainer> GetOrStartPostgresAsync(CancellationToken ct = default)
    {
        if (postgres != null) return postgres;

        await postgresLock.WaitAsync(ct);
        try
        {
            if (postgres != null) return postgres;

            var container = new PostgreSqlBuilder()
                .WithImage("postgres:16-alpine")
                .WithNetwork(Network)
                .WithNetworkAliases("postgres")
                .WithDatabase("postgres")
                .WithUsername(PostgresUsername)
                .WithPassword(PostgresPassword)
                // Default max_connections (100) is too low once several scenario containers -- each
                // with its own connection pool, plus JobMaster's own background master/agent
                // connections -- share this one instance, and a single Task.WhenAll schedule burst
                // (e.g. 150 concurrent job inserts) can approach that limit on its own.
                // No leading "postgres" here: docker-entrypoint.sh already prepends it when the
                // first arg starts with '-' -- passing it ourselves double-supplies it and postgres
                // rejects the resulting stray positional argument.
                .WithCommand("-c", "max_connections=400")
                .Build();

            await container.StartAsync(ct);
            postgres = container;
            return container;
        }
        finally
        {
            postgresLock.Release();
        }
    }

    public async Task<MySqlContainer> GetOrStartMySqlAsync(CancellationToken ct = default)
    {
        if (mySql != null) return mySql;

        await mySqlLock.WaitAsync(ct);
        try
        {
            if (mySql != null) return mySql;

            var container = new MySqlBuilder()
                .WithImage("mysql:8.0")
                .WithNetwork(Network)
                .WithNetworkAliases("mysql")
                .WithDatabase("mysql")
                .WithUsername(MySqlUsername)
                .WithPassword(MySqlPassword)
                // Same headroom reasoning as the Postgres container above: default max_connections
                // (151) is shared across every cluster's master + agent pools, across every
                // container. No leading "mysqld" -- docker-entrypoint.sh prepends it itself when the
                // first arg starts with '-', same as postgres's docker-entrypoint.sh.
                .WithCommand("--max-connections=400")
                .Build();

            await container.StartAsync(ct);
            mySql = container;
            return container;
        }
        finally
        {
            mySqlLock.Release();
        }
    }

    public async Task<MsSqlContainer> GetOrStartSqlServerAsync(CancellationToken ct = default)
    {
        if (sqlServer != null) return sqlServer;

        await sqlServerLock.WaitAsync(ct);
        try
        {
            if (sqlServer != null) return sqlServer;

            var container = new MsSqlBuilder()
                .WithNetwork(Network)
                .WithNetworkAliases("sqlserver")
                .WithPassword(SqlServerPassword)
                .Build();

            await container.StartAsync(ct);
            sqlServer = container;
            return container;
        }
        finally
        {
            sqlServerLock.Release();
        }
    }

    public async Task<RedisContainer> GetOrStartRedisAsync(CancellationToken ct = default)
    {
        if (redis != null) return redis;

        await redisLock.WaitAsync(ct);
        try
        {
            if (redis != null) return redis;

            var container = new RedisBuilder()
                .WithImage("redis:7-alpine")
                .WithNetwork(Network)
                .WithNetworkAliases("redis")
                .Build();

            await container.StartAsync(ct);
            redis = container;
            return container;
        }
        finally
        {
            redisLock.Release();
        }
    }

    public async Task<NatsContainer> GetOrStartNatsAsync(CancellationToken ct = default)
    {
        if (nats != null) return nats;

        await natsLock.WaitAsync(ct);
        try
        {
            if (nats != null) return nats;

            // JetStream is enabled by default (NatsBuilder always passes --jetstream); no extra
            // command flags needed here, unlike Postgres/MySql's max_connections bump above -- a
            // single scenario only ever runs one NATS-backed cluster's worth of agent connections
            // against this container, nowhere near JetStream's default limits.
            var container = new NatsBuilder()
                .WithImage("nats:2.10-alpine")
                .WithNetwork(Network)
                .WithNetworkAliases("nats")
                .WithUsername(NatsUsername)
                .WithPassword(NatsPassword)
                .Build();

            await container.StartAsync(ct);
            nats = container;
            return container;
        }
        finally
        {
            natsLock.Release();
        }
    }

    public async Task<RavenDbContainer> GetOrStartRavenDbAsync(CancellationToken ct = default)
    {
        if (ravenDb != null) return ravenDb;

        await ravenDbLock.WaitAsync(ct);
        try
        {
            if (ravenDb != null) return ravenDb;

            // Client major version (RavenDB.Client 7.x, referenced by JobMaster.RavenDb) must track the
            // server image's major version -- a mismatch rejects the client's request bodies with a
            // binary/JSON protocol error.
            //
            // CPU/memory capped to match RavenDB Community's own license ceiling (3 CPU / 6GB)
            const long ravenDbNanoCpus = 3_000_000_000; // 3 CPU
            const long ravenDbMemoryBytes = 6L * 1024 * 1024 * 1024; // 6GB
            var container = new RavenDbBuilder()
                .WithImage("ravendb/ravendb:7.2-ubuntu-latest")
                .WithNetwork(Network)
                .WithNetworkAliases("ravendb")
                .WithCreateParameterModifier(p =>
                {
                    p.HostConfig.NanoCPUs = ravenDbNanoCpus;
                    p.HostConfig.Memory = ravenDbMemoryBytes;
                })
                .Build();

            await container.StartAsync(ct);
            ravenDb = container;
            return container;
        }
        finally
        {
            ravenDbLock.Release();
        }
    }

    /// <summary>
    /// Builds (once per run, cached) and returns the image name for the app at <paramref name="dockerfilePath"/>.
    /// The image name is derived from the Dockerfile's parent folder (e.g.
    /// <c>Tests/TargetTestScheduleApp/Dockerfile</c> → <c>target-test-schedule-app:scenario-tests</c>),
    /// matching exactly what the two dedicated methods this replaced used to hardcode -- so every
    /// existing scenario's <c>dockerfilePath</c> JSON continues to resolve to the same image tag with
    /// zero changes required.
    /// </summary>
    public async Task<string> GetOrBuildAppImageAsync(string dockerfilePath, CancellationToken ct = default)
    {
        var key = dockerfilePath.Replace('\\', '/');

        if (images.TryGetValue(key, out var cached)) return cached.FullName!;

        await imageBuildLock.WaitAsync(ct);
        try
        {
            if (images.TryGetValue(key, out cached)) return cached.FullName!;

            var appFolder = Path.GetFileName(Path.GetDirectoryName(key)!.TrimEnd('/'));
            var imageName = $"{appFolder.ToKebabCase()}:scenario-tests";

            var repoRoot = RepoRootLocator.Find();
            var image = new ImageFromDockerfileBuilder()
                .WithDockerfileDirectory(new CommonDirectoryPath(repoRoot), "")
                .WithDockerfile(key)
                .WithName(imageName)
                .WithCleanUp(true)
                .Build();

            await image.CreateAsync(ct);
            images[key] = image;
            return image.FullName!;
        }
        finally
        {
            imageBuildLock.Release();
        }
    }

    public async Task DisposeAsync()
    {
        if (postgres != null) await postgres.DisposeAsync();
        if (mySql != null) await mySql.DisposeAsync();
        if (sqlServer != null) await sqlServer.DisposeAsync();
        if (redis != null) await redis.DisposeAsync();
        if (nats != null) await nats.DisposeAsync();
        if (ravenDb != null) await ravenDb.DisposeAsync();

        // Network may never have been assigned if InitializeAsync itself failed (e.g. Docker
        // unreachable) — don't let a NullReferenceException here mask that real error.
        if (Network != null) await Network.DeleteAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class ScenarioCollection : ICollectionFixture<ScenarioGlobalEnvironment>
{
    public const string Name = "ScenarioGlobalEnvironment";
}
