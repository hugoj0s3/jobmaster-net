using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
using DotNet.Testcontainers.Networks;
using Testcontainers.MsSql;
using Testcontainers.MySql;
using Testcontainers.Nats;
using Testcontainers.PostgreSql;
using Testcontainers.RavenDb;
using Testcontainers.Redis;

namespace JobMaster.Benchmarks.Common.Containers;

/// <summary>Which database engine backs the shared DB container -- Postgres for JobMaster's
/// PostgresPure config (the default, preserving existing behavior), SqlServer/MySql for frameworks
/// benchmarked across multiple database engines (e.g. Quartz.NET). RavenDB is named to match
/// exactly -- <c>RavenDbRepositoryConstants.RepositoryTypeId</c> ("RavenDB") -- since
/// JobMasterTopologyBuilder derives the JSON config's RepoType/RepositoryType strings straight from
/// <c>dbEngine.ToString()</c>, unlike a special-cased mapping for the other three.</summary>
public enum DbEngine
{
    Postgres,
    SqlServer,
    MySql,
    RavenDB,
}

/// <summary>
/// Single-Docker-host benchmark topology: one resource-limited DB container (2 CPU / 2GB, the
/// benchmark's baseline DB sizing), one unconstrained Redis container (benchmark plumbing -- latency and
/// completion recording -- not part of what's being measured, so not resource-limited), and N
/// resource-limited worker containers (0.5 CPU / 512MB each) built from a single app Dockerfile with
/// per-container environment variables (so each can be configured with a different JobMaster
/// cluster-config role: Coordinator+Drain vs. Execution-only).
///
/// Everything shares one Docker network exactly like <c>ScenarioGlobalEnvironment</c> does, just
/// built standalone here rather than through xUnit's <c>IAsyncLifetime</c> -- this environment is
/// created and torn down once per benchmark run (console app), not once per test collection.
/// </summary>
public sealed class BenchmarkContainerEnvironment : IAsyncDisposable
{
    private const long DbNanoCpus = 2_000_000_000; // 2 CPU
    private const long DbMemoryBytes = 2L * 1024 * 1024 * 1024; // 2GB
    private const long WorkerNanoCpus = 500_000_000; // 0.5 CPU
    private const long WorkerMemoryBytes = 512L * 1024 * 1024; // 512MB

    public const string DbNetworkAlias = "db";
    public const string DbDatabaseName = "benchmark";
    public const string DbUsername = "benchmark";
    public const int DbPort = 5432;

    // "sa" is the fixed admin username Testcontainers.MsSql's image expects.
    public const string SqlServerUsername = "sa";
    public const int SqlServerPort = 1433;

    public const string MySqlUsername = "root";
    public const int MySqlPort = 3306;

    public const string RedisNetworkAlias = "redis";

    public const string NatsNetworkAlias = "nats";
    public const string NatsUsername = "natsuser";
    public const int NatsPort = 4222;

    // Generated once per process (one benchmark run), never hardcoded -- SQL Server's password
    // complexity policy (3 of 4 character classes, 8+ chars) is satisfied by SecretGenerator's
    // base64url alphabet, which already spans upper/lower/digit/symbol.
    public static readonly string DbPassword = SecretGenerator.Generate();
    public static readonly string SqlServerPassword = SecretGenerator.Generate();
    public static readonly string MySqlPassword = SecretGenerator.Generate();
    public static readonly string NatsPassword = SecretGenerator.Generate();

    private INetwork? network;
    private readonly List<IContainer> workerContainers = [];
    private readonly List<IFutureDockerImage> builtImages = [];

    // IDatabaseContainer for the 3 SQL engines; RavenDbContainer doesn't implement IDatabaseContainer
    // (confirmed -- it's a different shape, GetConnectionString() is a concrete method, not an
    // interface member), so RavenDB gets its own field instead, mirroring how NatsContainer is
    // already separate. Exactly one of DbContainer/RavenDbContainer is ever populated per run.
    public IDatabaseContainer DbContainer { get; private set; } = null!;
    public RavenDbContainer? RavenDbContainer { get; private set; }
    public IContainer RedisContainer { get; private set; } = null!;
    public IContainer? NatsContainer { get; private set; }
    public IReadOnlyList<IContainer> WorkerContainers => workerContainers;

    /// <summary>Generic container identity for stats sampling / log capture, regardless of which DB
    /// engine is active -- both IDatabaseContainer and RavenDbContainer are themselves IContainer.</summary>
    public IContainer DbContainerForOps => RavenDbContainer is not null ? RavenDbContainer : (IContainer)DbContainer;

    public async Task StartAsync(
        IReadOnlyList<WorkerContainerSpec> workerSpecs,
        IReadOnlyList<string>? databaseNamesToProvision = null,
        DbEngine dbEngine = DbEngine.Postgres,
        bool includeNats = false,
        Func<IDatabaseContainer, Task>? afterDbProvisionedAsync = null,
        // Defaults preserve the original fixed 2 CPU / 2GB DB spec -- overridable per run so larger
        // burst-capacity tiers can scale the DB up (capped at 16GB) alongside
        // worker count, without changing behavior for every existing paced-suite call site.
        long? dbNanoCpus = null,
        long? dbMemoryBytes = null,
        CancellationToken ct = default)
    {
        network = new NetworkBuilder().Build();
        await network.CreateAsync(ct);

        var effectiveDbNanoCpus = dbNanoCpus ?? DbNanoCpus;
        var effectiveDbMemoryBytes = dbMemoryBytes ?? DbMemoryBytes;

        if (dbEngine == DbEngine.RavenDB)
        {
            RavenDbContainer = await StartRavenDbAsync(databaseNamesToProvision, effectiveDbNanoCpus, effectiveDbMemoryBytes, ct);
        }
        else
        {
            DbContainer = dbEngine switch
            {
                DbEngine.SqlServer => await StartSqlServerAsync(databaseNamesToProvision, effectiveDbNanoCpus, effectiveDbMemoryBytes, ct),
                DbEngine.MySql => await StartMySqlAsync(databaseNamesToProvision, effectiveDbNanoCpus, effectiveDbMemoryBytes, ct),
                _ => await StartPostgresAsync(databaseNamesToProvision, effectiveDbNanoCpus, effectiveDbMemoryBytes, ct),
            };
        }

        // Extension point for schema that a framework doesn't create itself (e.g. Quartz.NET's
        // AdoJobStore, which requires QRTZ_* tables to exist up front, unlike JobMaster's/Hangfire's
        // self-migrating schema) -- runs after the database exists but before any worker container
        // starts, so no container races another to use a not-yet-provisioned schema.
        if (afterDbProvisionedAsync is not null)
        {
            await afterDbProvisionedAsync(DbContainer);
        }

        RedisContainer = new RedisBuilder()
            .WithImage("redis:7-alpine")
            .WithNetwork(network)
            .WithNetworkAliases(RedisNetworkAlias)
            .Build();
        await RedisContainer.StartAsync(ct);

        // Additive to the SQL master DB above, not a replacement -- JobMaster's "*Nats" configs keep
        // the master/coordinator data in the chosen SQL engine and only swap each agent connection's
        // RepositoryType to NatsJetStream (see JobMasterTopologyBuilder). No provisioning step needed
        // here, unlike the SQL providers: JetStream has no CREATE DATABASE-equivalent -- the app
        // provisions its own streams at startup, same as ScenarioGlobalEnvironment's NATS container.
        if (includeNats)
        {
            NatsContainer = new NatsBuilder()
                .WithImage("nats:2.10-alpine")
                .WithNetwork(network)
                .WithNetworkAliases(NatsNetworkAlias)
                .WithUsername(NatsUsername)
                .WithPassword(NatsPassword)
                .Build();
            await NatsContainer.StartAsync(ct);
        }

        var imageCache = new Dictionary<string, string>();

        foreach (var spec in workerSpecs)
        {
            if (!imageCache.TryGetValue(spec.DockerfilePath, out var imageName))
            {
                var (name, image) = await BuildImageAsync(spec.DockerfilePath, ct);
                imageName = name;
                imageCache[spec.DockerfilePath] = imageName;
                builtImages.Add(image);
            }

            var builder = new ContainerBuilder()
                .WithImage(imageName)
                .WithNetwork(network)
                .WithNetworkAliases(spec.Name)
                .WithPortBinding(spec.HttpPort, true)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r
                    .ForPath(spec.HealthCheckPath)
                    .ForPort((ushort)spec.HttpPort)))
                .WithCreateParameterModifier(p =>
                {
                    p.HostConfig.NanoCPUs = WorkerNanoCpus;
                    p.HostConfig.Memory = WorkerMemoryBytes;
                });

            foreach (var (key, value) in spec.EnvironmentVariables)
            {
                builder = builder.WithEnvironment(key, value);
            }

            var container = builder.Build();
            workerContainers.Add(container);
            await container.StartAsync(ct);
        }
    }

    public string GetWorkerBaseUrl(int index, int httpPort = 8080)
    {
        var container = workerContainers[index];
        return $"http://localhost:{container.GetMappedPublicPort(httpPort)}";
    }

    private async Task<IDatabaseContainer> StartPostgresAsync(IReadOnlyList<string>? databaseNamesToProvision, long dbNanoCpus, long dbMemoryBytes, CancellationToken ct)
    {
        var container = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .WithNetwork(network)
            .WithNetworkAliases(DbNetworkAlias)
            .WithDatabase(DbDatabaseName)
            .WithUsername(DbUsername)
            .WithPassword(DbPassword)
            // Default max_connections (100) is too low once every worker container opens pooled
            // connections to every agent database plus the master database under heavy concurrent
            // load. 500 was tried first and confirmed insufficient (real "sorry, too many clients
            // already" failures in Postgres-Pure mode with ~15-20 executors, each holding a separate
            // Maximum Pool Size=25 pool to both its own agent database and the master database --
            // e.g. 20 executors alone can demand up to 20*25*2=1000 connections at peak, well past
            // 500, even before counting the coordinator/drainer containers). 2000 is a generous fixed
            // ceiling to avoid tuning this per executor count -- Postgres-NATS mode never gets close
            // to it (no per-executor agent-database pool at all), so this only matters for Pure mode.
            // Set via the create parameter modifier rather than .WithCommand(...): .WithCommand(...)
            // breaks PostgreSqlBuilder's own readiness-wait strategy (container reports "not running"
            // on every attempt), even though the same command/env/resource-limits combination works
            // fine via a raw docker run/exec.
            .WithCreateParameterModifier(p =>
            {
                p.HostConfig.NanoCPUs = dbNanoCpus;
                p.HostConfig.Memory = dbMemoryBytes;
                var cmd = p.Cmd?.ToList() ?? [];
                cmd.AddRange(["-c", "max_connections=2000"]);
                p.Cmd = cmd;
            })
            .Build();
        await container.StartAsync(ct);

        // Postgres requires each database to exist before a worker container connects to it --
        // provisioned here, before any worker container starts, using the DB container's
        // host-mapped connection string (this runs on the host machine, not inside the network).
        if (databaseNamesToProvision is { Count: > 0 })
        {
            await PostgresDatabaseProvisioner.CreateDatabasesIfNotExistsAsync(container.GetConnectionString(), databaseNamesToProvision, ct);
        }

        return container;
    }

    private async Task<IDatabaseContainer> StartSqlServerAsync(IReadOnlyList<string>? databaseNamesToProvision, long dbNanoCpus, long dbMemoryBytes, CancellationToken ct)
    {
        var container = new MsSqlBuilder()
            .WithNetwork(network)
            .WithNetworkAliases(DbNetworkAlias)
            .WithPassword(SqlServerPassword)
            .WithCreateParameterModifier(p =>
            {
                p.HostConfig.NanoCPUs = dbNanoCpus;
                p.HostConfig.Memory = dbMemoryBytes;
            })
            .Build();
        await container.StartAsync(ct);

        // Mirrors the Postgres path above -- SQL Server also requires each database to exist before
        // a worker container connects to it.
        if (databaseNamesToProvision is { Count: > 0 })
        {
            await SqlServerDatabaseProvisioner.CreateDatabasesIfNotExistsAsync(container.GetConnectionString(), databaseNamesToProvision, ct);
        }

        return container;
    }

    private async Task<IDatabaseContainer> StartMySqlAsync(IReadOnlyList<string>? databaseNamesToProvision, long dbNanoCpus, long dbMemoryBytes, CancellationToken ct)
    {
        var container = new MySqlBuilder()
            .WithImage("mysql:8.0")
            .WithNetwork(network)
            .WithNetworkAliases(DbNetworkAlias)
            .WithDatabase("mysql")
            .WithUsername(MySqlUsername)
            .WithPassword(MySqlPassword)
            // No leading "mysqld" -- docker-entrypoint.sh prepends it itself when the first arg
            // starts with '-', matching ScenarioGlobalEnvironment's MySQL setup. Unlike Postgres,
            // WithCommand doesn't break MsSqlBuilder's/MySqlBuilder's readiness wait here.
            .WithCommand("--max-connections=400")
            .WithCreateParameterModifier(p =>
            {
                p.HostConfig.NanoCPUs = dbNanoCpus;
                p.HostConfig.Memory = dbMemoryBytes;
            })
            .Build();
        await container.StartAsync(ct);

        // Mirrors the Postgres/SqlServer paths above -- MySQL also requires each database to exist
        // before a worker container connects to it.
        if (databaseNamesToProvision is { Count: > 0 })
        {
            await MySqlDatabaseProvisioner.CreateDatabasesIfNotExistsAsync(container.GetConnectionString(), databaseNamesToProvision, ct);
        }

        return container;
    }

    private async Task<RavenDbContainer> StartRavenDbAsync(IReadOnlyList<string>? databaseNamesToProvision, long dbNanoCpus, long dbMemoryBytes, CancellationToken ct)
    {
        var container = new RavenDbBuilder()
            .WithImage("ravendb/ravendb:7.2-ubuntu-latest")
            .WithNetwork(network)
            .WithNetworkAliases(DbNetworkAlias)
            .WithCreateParameterModifier(p =>
            {
                p.HostConfig.NanoCPUs = dbNanoCpus;
                p.HostConfig.Memory = dbMemoryBytes;
            })
            .Build();
        await container.StartAsync(ct);

        // Mirrors the SQL paths above -- RavenDB also requires each database to exist before a
        // worker container connects to it (unlike JobMaster's own schema/collections, which are
        // implicit and need no separate provisioning step).
        if (databaseNamesToProvision is { Count: > 0 })
        {
            await RavenDbDatabaseProvisioner.CreateDatabasesIfNotExistsAsync(container.GetConnectionString(), databaseNamesToProvision, ct);
        }

        return container;
    }

    // Returns the built IFutureDockerImage alongside its name, not just the name -- WithCleanUp(true)
    // below only actually removes the image when this object is disposed, and the caller needs to hold
    // onto it (in builtImages) to do that in DisposeAsync(). Previously this returned just the name and
    // let the image object fall out of scope undisposed, so WithCleanUp(true) never fired -- combined
    // with every run reusing the same fixed tag (e.g. "jobmasterhost:benchmarks", no run-ID), each
    // rebuild just moved the tag onto the new image and left the previous one behind as a dangling,
    // untagged image still consuming disk. Confirmed as the cause of a real host-disk-exhaustion incident
    // that crashed Docker mid-run.
    private static async Task<(string Name, IFutureDockerImage Image)> BuildImageAsync(string dockerfilePath, CancellationToken ct)
    {
        var key = dockerfilePath.Replace('\\', '/');
        var repoRoot = RepoRootLocator.Find();
        var imageName = $"{Path.GetFileName(Path.GetDirectoryName(key)!.TrimEnd('/')).ToLowerInvariant()}:benchmarks";

        var image = new ImageFromDockerfileBuilder()
            .WithDockerfileDirectory(new CommonDirectoryPath(repoRoot), "")
            .WithDockerfile(key)
            .WithName(imageName)
            .WithCleanUp(true)
            .Build();

        await image.CreateAsync(ct);
        return (image.FullName!, image);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var container in workerContainers)
        {
            await container.DisposeAsync();
        }

        // Must come after the worker containers above -- they're still running off this image until
        // disposed. See BuildImageAsync's comment for why this step didn't exist before.
        foreach (var image in builtImages)
        {
            await image.DisposeAsync();
        }

        if (RedisContainer is not null) await RedisContainer.DisposeAsync();
        if (NatsContainer is not null) await NatsContainer.DisposeAsync();
        // IDatabaseContainer itself doesn't expose DisposeAsync -- both concrete container types
        // (PostgreSqlContainer, MsSqlContainer) implement IAsyncDisposable via their shared
        // DockerContainer base class, so this cast always succeeds at runtime.
        if (DbContainer is IAsyncDisposable disposableDb) await disposableDb.DisposeAsync();
        if (RavenDbContainer is not null) await RavenDbContainer.DisposeAsync();
        if (network is not null) await network.DeleteAsync();
    }
}
