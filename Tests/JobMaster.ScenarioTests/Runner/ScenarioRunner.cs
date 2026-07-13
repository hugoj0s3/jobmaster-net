using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using JobMaster.ScenarioTests.Fixtures;
using StackExchange.Redis;

namespace JobMaster.ScenarioTests.Runner;

public sealed class ScenarioRunner : IAsyncDisposable
{
    private readonly ScenarioGlobalEnvironment global;
    private readonly string scenarioFolder;
    private readonly Dictionary<string, IContainer> runningContainers = new();
    private readonly Dictionary<string, string> containerBaseUrls = new();
    private readonly ScenarioYarpProxy proxy = new();

    private ScenarioDefinition definition = null!;
    private HttpClient? apiHttpClient;
    private IConnectionMultiplexer? redisMux;

    // Generated once per scenario run, never authored as literals in scenario JSON.
    private readonly string apiKey = SecretGenerator.Generate();
    private readonly string apiUsername = "scenario-user";
    private readonly string apiPassword = SecretGenerator.Generate();

    public IExecutionTracker Tracker { get; private set; } = null!;
    public IScheduleClient Schedule { get; private set; } = null!;
    public IScenarioApiClient? Api { get; private set; }

    /// <summary>
    /// The api container's base address, exposed so a test can build its own bare HttpClient (no
    /// default headers) when it needs to isolate one auth mechanism from another — e.g. proving
    /// JWT auth is enforced on its own, without the API-key default header on <see cref="Api"/>'s
    /// underlying client masking the result.
    /// </summary>
    public Uri? ApiBaseAddress => apiHttpClient?.BaseAddress;

    /// <summary>
    /// A schedule client pointed directly at one named container, bypassing the YARP proxy. The
    /// proxy round-robins requests across every schedule-app backend, which is correct when
    /// multiple containers serve the *same* cluster (horizontal scaling) but wrong when different
    /// containers each own a *different* cluster — a request for one cluster could land on a
    /// container that never registered it. Use this whenever a scenario runs multiple distinct
    /// clusters and needs to target a specific one.
    /// </summary>
    public IScheduleClient ScheduleFor(string containerName)
    {
        if (!containerBaseUrls.TryGetValue(containerName, out var baseUrl))
        {
            throw new InvalidOperationException($"Container '{containerName}' is not running.");
        }

        return new ScheduleClient(new HttpClient { BaseAddress = new Uri(baseUrl) });
    }

    private ScenarioRunner(ScenarioGlobalEnvironment global, string scenarioFolder)
    {
        this.global = global;
        this.scenarioFolder = scenarioFolder;
    }

    public static async Task<ScenarioRunner> StartAsync(
        ScenarioGlobalEnvironment global, string scenarioName, CancellationToken ct = default)
    {
        // scenarioName is the logical (kebab-case) identifier; the physical folder is its PascalCase form.
        var scenarioFolder = Path.Combine(RepoRootLocator.Find(), "Tests", "JobMaster.ScenarioTests", "Scenarios", scenarioName.ToPascalCase());
        var runner = new ScenarioRunner(global, scenarioFolder);
        await runner.InitializeAsync(ct);
        return runner;
    }

    private async Task InitializeAsync(CancellationToken ct)
    {
        var scenarioJsonPath = Path.Combine(scenarioFolder, "scenario.json");
        var scenarioJsonText = await File.ReadAllTextAsync(scenarioJsonPath, ct);
        definition = JsonSerializer.Deserialize<ScenarioDefinition>(scenarioJsonText, ScenarioJsonOptions.Default)
            ?? throw new InvalidOperationException($"Could not parse {scenarioJsonPath}");

        if (definition.Infrastructure.Count > 0)
        {
            throw new NotSupportedException(
                "Scenario-scoped infrastructure (e.g. NATS) is not implemented yet. " +
                $"Scenario '{definition.ScenarioName}' declares: {string.Join(", ", definition.Infrastructure.Select(i => i.Type))}.");
        }

        var redis = await global.GetOrStartRedisAsync(ct);
        redisMux = await ConnectionMultiplexer.ConnectAsync($"{redis.Hostname}:{redis.GetMappedPublicPort(6379)}");
        Tracker = new ExecutionTracker(redisMux);

        await proxy.StartAsync(ct);
        Schedule = new ScheduleClient(new HttpClient { BaseAddress = new Uri(proxy.ProxyUrl) });

        if (definition.Api != null)
        {
            var apiJsonPath = Path.Combine(scenarioFolder, "api.json");
            await StartContainerAsync(apiJsonPath, isApiContainer: true, ct);
        }
    }

    public async Task StartPhaseAsync(string phaseName, CancellationToken ct = default)
    {
        var phase = definition.Phases.FirstOrDefault(p => p.Name == phaseName)
            ?? throw new InvalidOperationException($"Phase '{phaseName}' not found in scenario '{definition.ScenarioName}'.");

        // phaseName (the lookup key, matching scenario.json's kebab-case phase "name") and the
        // physical phase folder (PascalCase, e.g. "Phase1") are different strings by convention.
        var phaseFolder = phaseName.ToPascalCase();

        foreach (var containerName in phase.Containers)
        {
            var containerJsonPath = Path.Combine(scenarioFolder, phaseFolder, containerName + ".json");
            await StartContainerAsync(containerJsonPath, isApiContainer: false, ct);
        }
    }

    public async Task StopAsync(string containerName, CancellationToken ct = default)
    {
        if (!runningContainers.TryGetValue(containerName, out var container))
        {
            return;
        }

        proxy.RemoveBackend(containerName);
        await container.StopAsync(ct);
        await container.DisposeAsync();
        runningContainers.Remove(containerName);
        containerBaseUrls.Remove(containerName);
    }

    private async Task StartContainerAsync(string containerJsonPath, bool isApiContainer, CancellationToken ct)
    {
        var tokens = BuildTokens();
        var rawJson = await File.ReadAllTextAsync(containerJsonPath, ct);
        var renderedJson = ConfigTemplateRenderer.Render(rawJson, tokens);

        var containerDef = JsonSerializer.Deserialize<ContainerDefinition>(renderedJson, ScenarioJsonOptions.Default)
            ?? throw new InvalidOperationException($"Could not parse {containerJsonPath}");

        await EnsureDatabasesForContainerAsync(containerDef, ct);

        var imageName = isApiContainer
            ? await global.GetOrBuildApiAppImageAsync(ct)
            : await global.GetOrBuildScheduleAppImageAsync(ct);

        var clusterConfigsJson = "[" + string.Join(",", containerDef.ClusterConfigTemplates.Select(e => e.GetRawText())) + "]";

        var builder = new ContainerBuilder()
            .WithImage(imageName)
            .WithNetwork(global.Network)
            .WithNetworkAliases(containerDef.NetworkAlias)
            .WithPortBinding((ushort)containerDef.HttpPort, true)
            .WithEnvironment("JOBMASTER_CLUSTER_CONFIGS_JSON", clusterConfigsJson)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r
                .ForPort((ushort)containerDef.HttpPort)
                .ForPath(containerDef.HealthCheckPath)));

        if (isApiContainer)
        {
            builder = builder.WithEnvironment(
                "JOBMASTER_API_REQUIRE_AUTH",
                (containerDef.Auth?.RequireAuthentication ?? false) ? "true" : "false");

            if (!string.IsNullOrEmpty(containerDef.Auth?.ApiKey))
            {
                builder = builder.WithEnvironment("JOBMASTER_API_KEY", containerDef.Auth.ApiKey);
            }

            if (!string.IsNullOrEmpty(containerDef.Auth?.Username) && !string.IsNullOrEmpty(containerDef.Auth.Password))
            {
                builder = builder
                    .WithEnvironment("JOBMASTER_API_USERNAME", containerDef.Auth.Username)
                    .WithEnvironment("JOBMASTER_API_PASSWORD", containerDef.Auth.Password);
            }

            if (!string.IsNullOrEmpty(containerDef.ApiBasePath))
            {
                builder = builder.WithEnvironment("JOBMASTER_API_BASE_PATH", containerDef.ApiBasePath);
            }

            if (containerDef.Auth?.EnableJwt ?? false)
            {
                builder = builder.WithEnvironment("JOBMASTER_API_ENABLE_JWT", "true");
            }
        }
        else
        {
            builder = builder.WithEnvironment("REDIS_CONNECTION_STRING", "redis:6379");
        }

        var container = builder.Build();

        using var startupCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        startupCts.CancelAfter(TimeSpan.FromSeconds(90));
        await container.StartAsync(startupCts.Token);

        runningContainers[containerDef.ContainerName] = container;

        var hostBaseUrl = $"http://{container.Hostname}:{container.GetMappedPublicPort(containerDef.HttpPort)}";
        containerBaseUrls[containerDef.ContainerName] = hostBaseUrl;

        if (isApiContainer)
        {
            apiHttpClient = new HttpClient { BaseAddress = new Uri(hostBaseUrl) };

            if (!string.IsNullOrEmpty(containerDef.Auth?.ApiKey))
            {
                apiHttpClient.DefaultRequestHeaders.Add("x-api-key", containerDef.Auth.ApiKey);
            }

            if (!string.IsNullOrEmpty(containerDef.Auth?.Username) && !string.IsNullOrEmpty(containerDef.Auth.Password))
            {
                apiHttpClient.DefaultRequestHeaders.Add("X-User-Name", containerDef.Auth.Username);
                apiHttpClient.DefaultRequestHeaders.Add("X-Password", containerDef.Auth.Password);
            }

            Api = new ScenarioApiClient(apiHttpClient, containerDef.ApiBasePath ?? "/jm-api");
        }
        else
        {
            proxy.AddBackend(containerDef.ContainerName, hostBaseUrl);
        }
    }

    private Dictionary<string, string> BuildTokens()
    {
        return new Dictionary<string, string>
        {
            ["PostgresHost"] = "postgres",
            ["PostgresPort"] = "5432",
            ["PostgresUsername"] = ScenarioGlobalEnvironment.PostgresUsername,
            ["PostgresPassword"] = global.PostgresPassword,
            ["MySqlHost"] = "mysql",
            ["MySqlPort"] = "3306",
            ["MySqlUsername"] = ScenarioGlobalEnvironment.MySqlUsername,
            ["MySqlPassword"] = global.MySqlPassword,
            ["SqlServerHost"] = "sqlserver",
            ["SqlServerPort"] = "1433",
            ["SqlServerUsername"] = ScenarioGlobalEnvironment.SqlServerUsername,
            ["SqlServerPassword"] = global.SqlServerPassword,
            ["ApiKey"] = apiKey,
            ["ApiUsername"] = apiUsername,
            ["ApiPassword"] = apiPassword
        };
    }

    /// <summary>
    /// Lazily starts and provisions only the shared database engine(s) this container's rendered
    /// cluster configs actually reference (master RepoType + every agent connection's
    /// RepositoryType) — so a Postgres-only scenario never pays for spinning up MySql/SqlServer,
    /// and vice versa.
    /// </summary>
    private async Task EnsureDatabasesForContainerAsync(ContainerDefinition containerDef, CancellationToken ct)
    {
        var repoTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var clusterConfig in containerDef.ClusterConfigTemplates)
        {
            if (clusterConfig.TryGetProperty("RepoType", out var repoType) && repoType.ValueKind == JsonValueKind.String)
            {
                repoTypes.Add(repoType.GetString()!);
            }

            if (clusterConfig.TryGetProperty("AgentConnections", out var agentConnections) && agentConnections.ValueKind == JsonValueKind.Array)
            {
                foreach (var agentConnection in agentConnections.EnumerateArray())
                {
                    if (agentConnection.TryGetProperty("RepositoryType", out var agentRepoType) && agentRepoType.ValueKind == JsonValueKind.String)
                    {
                        repoTypes.Add(agentRepoType.GetString()!);
                    }
                }
            }
        }

        foreach (var repoType in repoTypes)
        {
            if (string.Equals(repoType, "Postgres", StringComparison.OrdinalIgnoreCase))
            {
                var postgres = await global.GetOrStartPostgresAsync(ct);
                await PostgresDatabaseProvisioner.CreateDatabasesIfNotExistsAsync(postgres.GetConnectionString(), ct);
            }
            else if (string.Equals(repoType, "MySql", StringComparison.OrdinalIgnoreCase))
            {
                var mySql = await global.GetOrStartMySqlAsync(ct);
                await MySqlDatabaseProvisioner.CreateDatabasesIfNotExistsAsync(mySql.GetConnectionString(), ct);
            }
            else if (string.Equals(repoType, "SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                var sqlServer = await global.GetOrStartSqlServerAsync(ct);
                await SqlServerDatabaseProvisioner.CreateDatabasesIfNotExistsAsync(sqlServer.GetConnectionString(), ct);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var container in runningContainers.Values)
        {
            await container.DisposeAsync();
        }

        await proxy.DisposeAsync();
        apiHttpClient?.Dispose();

        if (redisMux != null)
        {
            await redisMux.CloseAsync();
            redisMux.Dispose();
        }
    }
}
