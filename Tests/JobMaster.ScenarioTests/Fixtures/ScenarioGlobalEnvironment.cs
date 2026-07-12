using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Images;
using DotNet.Testcontainers.Networks;
using JobMaster.ScenarioTests.Runner;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;
using Xunit;

namespace JobMaster.ScenarioTests.Fixtures;

/// <summary>
/// Shared, run-scoped infrastructure: one Postgres and one Redis container for the entire test
/// run (lazily started on first use), plus the TargetTestScheduleApp/TargetTestApi images built
/// once and reused by every scenario. Disposed only when the whole test run ends.
/// </summary>
public sealed class ScenarioGlobalEnvironment : IAsyncLifetime
{
    public const string PostgresUsername = "postgres";

    /// <summary>Generated once per test run — never hardcoded, never logged.</summary>
    public string PostgresPassword { get; } = SecretGenerator.Generate();

    private readonly SemaphoreSlim postgresLock = new(1, 1);
    private readonly SemaphoreSlim redisLock = new(1, 1);
    private readonly SemaphoreSlim scheduleImageLock = new(1, 1);
    private readonly SemaphoreSlim apiImageLock = new(1, 1);

    private PostgreSqlContainer? postgres;
    private RedisContainer? redis;
    private IFutureDockerImage? scheduleAppImage;
    private IFutureDockerImage? apiAppImage;

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

    public async Task<string> GetOrBuildScheduleAppImageAsync(CancellationToken ct = default)
    {
        if (scheduleAppImage != null) return scheduleAppImage.FullName!;

        await scheduleImageLock.WaitAsync(ct);
        try
        {
            if (scheduleAppImage != null) return scheduleAppImage.FullName!;

            var repoRoot = RepoRootLocator.Find();
            var image = new ImageFromDockerfileBuilder()
                .WithDockerfileDirectory(new CommonDirectoryPath(repoRoot), "")
                .WithDockerfile("Tests/TargetTestScheduleApp/Dockerfile")
                .WithName("target-test-schedule-app:scenario-tests")
                .WithCleanUp(true)
                .Build();

            await image.CreateAsync(ct);
            scheduleAppImage = image;
            return image.FullName!;
        }
        finally
        {
            scheduleImageLock.Release();
        }
    }

    public async Task<string> GetOrBuildApiAppImageAsync(CancellationToken ct = default)
    {
        if (apiAppImage != null) return apiAppImage.FullName!;

        await apiImageLock.WaitAsync(ct);
        try
        {
            if (apiAppImage != null) return apiAppImage.FullName!;

            var repoRoot = RepoRootLocator.Find();
            var image = new ImageFromDockerfileBuilder()
                .WithDockerfileDirectory(new CommonDirectoryPath(repoRoot), "")
                .WithDockerfile("Tests/TargetTestApi/Dockerfile")
                .WithName("target-test-api:scenario-tests")
                .WithCleanUp(true)
                .Build();

            await image.CreateAsync(ct);
            apiAppImage = image;
            return image.FullName!;
        }
        finally
        {
            apiImageLock.Release();
        }
    }

    public async Task DisposeAsync()
    {
        if (postgres != null) await postgres.DisposeAsync();
        if (redis != null) await redis.DisposeAsync();

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
