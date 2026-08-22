using System.Text.Json;
using JobMaster.Benchmarks.Common.Containers;

namespace JobMaster.Benchmarks.Runner;

/// <summary>
/// Builds one JobMaster cluster-config JSON per container. Coordinator instances (<paramref
/// name="coordinatorCount"/> total, in <see cref="BuildWorkerSpecs"/>'s <c>coordinatorContainerCount</c>
/// dedicated containers, split as evenly as possible) and the single Drain instance are always in
/// their own separate containers from each other and from every Execution container -- this isolates
/// each role's resource usage (container CPU/memory limits apply per-container) instead of bundling
/// everything management-plane into one shared container.
///
/// Every container's config declares ALL agent connections in the cluster, not just its own --
/// the coordinator needs to recognize every executor's <c>AgentConnectionId</c> to select their
/// buckets (<c>MasterBucketService.FilterApplicableBuckets</c> rejects any bucket whose connection
/// isn't in the calling process's own <c>ClusterConnConfig</c>). Coordinator-only containers don't
/// set their own <c>AgentConnectionName</c> (coordinators operate on the master DB directly, not a
/// bucket), so they need no dedicated agent connection/database of their own -- only the drainer and
/// each executor do.
///
/// Each execution container gets <paramref name="bucketsPerWorker"/> (default 1) Medium buckets --
/// matches JobMaster's out-of-the-box default of 1 bucket per priority per worker.
///
/// NATS (<paramref name="useNats"/>) is additive: it only swaps every AgentConnection's
/// RepositoryType to "NatsJetStream" -- master data always stays on <paramref name="dbEngine"/>.
/// </summary>
public static class JobMasterTopologyBuilder
{
    private const string ClusterId = "benchmark";
    private const string TransientThreshold = "00:02:00";
    private const string DrainerAgentConnectionName = "agent-drainer";
    private const string SharedAgentConnectionName = "agent-shared";

    /// <summary>In-container path for a coordinator/drainer container's optional debug JSONL mirror
    /// log -- copied out to container-logs/ after the run when enabled. See
    /// <see cref="BuildWorkerSpecs"/>'s <c>enableDebugJsonl</c> parameter.</summary>
    public const string DebugJsonlContainerPath = "/app/debug-log.jsonl";

    // Every job in this benchmark schedules at the default JobMasterPriority.Medium (NoOpBenchmarkHandler
    // carries no [JobMasterPriority] attribute, and the schedule endpoints pass none explicitly) --
    // disabling the other priorities avoids the framework standing up buckets/runners for priority
    // lanes this benchmark never uses. Confirmed empirically to not affect throughput/latency on its
    // own (isolated test showed no measurable difference) -- kept anyway since it's still correct
    // and harmless to leave disabled.
    private static readonly string[] DisabledPriorities = ["VeryLow", "Low", "High", "Critical"];

    public static IReadOnlyList<WorkerContainerSpec> BuildWorkerSpecs(
        DbEngine dbEngine,
        bool useNats,
        int executorCount,
        string runId,
        int bucketsPerWorker = 1,
        int? bucketBufferSize = null,
        bool skipWarmUpTime = false,
        bool sharedAgentConnection = false,
        int coordinatorCount = 4,
        int coordinatorContainerCount = 1,
        int? transferBatchSize = null,
        bool enableDebugJsonl = false,
        double? parallelismFactor = null)
    {
        if (executorCount < 1)
        {
            throw new ArgumentException("executorCount must be at least 1.", nameof(executorCount));
        }

        if (coordinatorContainerCount < 1)
        {
            throw new ArgumentException("coordinatorContainerCount must be at least 1.", nameof(coordinatorContainerCount));
        }

        if (coordinatorCount < coordinatorContainerCount)
        {
            throw new ArgumentException(
                "coordinatorCount must be at least coordinatorContainerCount (each coordinator container needs at least 1 coordinator instance).",
                nameof(coordinatorCount));
        }

        var specs = new List<WorkerContainerSpec>(coordinatorContainerCount + 1 + executorCount);

        var masterConnectionString = BuildConnectionString(dbEngine, BenchmarkContainerEnvironment.DbDatabaseName);

        var executorAgentConnectionNames = Enumerable.Range(0, executorCount).Select(i => $"agent-exec-{i}").ToArray();

        // Every container's config gets this same full list -- see the class remarks on why a
        // coordinator that only knows its own connection can never select an executor's bucket, and
        // on why coordinator-only containers don't need one of their own. sharedAgentConnection
        // collapses this down to exactly one entry, used by the drainer and every executor -- buckets
        // (not separate databases) are the real partition unit between workers, so this is
        // architecturally valid; it exists to isolate "database count" as its own benchmark variable,
        // separate from bucket count.
        var allAgentConnections = sharedAgentConnection
            ? new object[]
            {
                useNats
                    ? new { Name = SharedAgentConnectionName, RepositoryType = "NatsJetStream", ConnectionString = NatsConnectionString() }
                    : new { Name = SharedAgentConnectionName, RepositoryType = dbEngine.ToString(), ConnectionString = BuildConnectionString(dbEngine, "benchmark_agent") },
            }
            : new[] { DrainerAgentConnectionName }.Concat(executorAgentConnectionNames)
                .Select(name => useNats
                    ? (object)new { Name = name, RepositoryType = "NatsJetStream", ConnectionString = NatsConnectionString() }
                    : new { Name = name, RepositoryType = dbEngine.ToString(), ConnectionString = BuildConnectionString(dbEngine, DatabaseNameFor(name)) })
                .ToArray();

        // Split coordinatorCount as evenly as possible across coordinatorContainerCount dedicated
        // containers -- the first (coordinatorCount % coordinatorContainerCount) containers get one
        // extra instance, so no single container ends up with a disproportionate share.
        var baseCoordinatorsPerContainer = coordinatorCount / coordinatorContainerCount;
        var coordinatorRemainder = coordinatorCount % coordinatorContainerCount;
        var coordinatorInstanceNumber = 0;
        for (var c = 0; c < coordinatorContainerCount; c++)
        {
            var instancesInThisContainer = baseCoordinatorsPerContainer + (c < coordinatorRemainder ? 1 : 0);
            List<object> coordinatorWorkers = [];
            for (var k = 0; k < instancesInThisContainer; k++)
            {
                coordinatorInstanceNumber++;
                // AssignJobsToBucketsRunner's imminent-path lock is keyed by a shared 10-second
                // wall-clock window (doesn't scale with instance count), but the scan-plan path
                // (delayed jobs) picks a random lock key from a range that widens with
                // CountActiveCoordinatorWorkersAsync(), so multiple instances genuinely divide that
                // work. transferBatchSize defaults to null (SDK's 1000 default) -- raise with care,
                // since a large batch can put real pressure on the DB and risks duplicate dispatches
                // if the claim lock expires mid-batch.
                coordinatorWorkers.Add(new { WorkerName = $"coordinator-{coordinatorInstanceNumber}", WorkerMode = "Coordinator", SkipWarmUpTime = skipWarmUpTime, TransferBatchSize = transferBatchSize });
            }

            specs.Add(BuildSpec($"coordinator-{c}", dbEngine, masterConnectionString, allAgentConnections, coordinatorWorkers, enableDebugJsonl, runId));
        }

        // Drainer -- always its own dedicated container, never bundled with coordinators.
        var drainerAgentConnectionName = sharedAgentConnection ? SharedAgentConnectionName : DrainerAgentConnectionName;
        List<object> drainerWorkers = [new { WorkerName = "drainer", AgentConnectionName = drainerAgentConnectionName, WorkerMode = "Drain", SkipWarmUpTime = skipWarmUpTime }];
        specs.Add(BuildSpec("drainer", dbEngine, masterConnectionString, allAgentConnections, drainerWorkers, enableDebugJsonl, runId));

        // Executors -- one container each, execution-only.
        for (var i = 0; i < executorCount; i++)
        {
            var agentConnectionName = sharedAgentConnection ? SharedAgentConnectionName : executorAgentConnectionNames[i];
            var bucketQtyConfig = new Dictionary<string, int> { ["Medium"] = bucketsPerWorker };
            List<object> executorWorkers = [new { WorkerName = "executor", AgentConnectionName = agentConnectionName, WorkerMode = "Execution", BucketQtyConfig = bucketQtyConfig, BucketBufferSize = bucketBufferSize, SkipWarmUpTime = skipWarmUpTime, ParallelismFactor = parallelismFactor }];
            specs.Add(BuildSpec($"executor-{i}", dbEngine, masterConnectionString, allAgentConnections, executorWorkers, enableDebugJsonl: false, runId));
        }

        return specs;
    }

    private static WorkerContainerSpec BuildSpec(
        string containerName,
        DbEngine dbEngine,
        string masterConnectionString,
        object[] allAgentConnections,
        List<object> workers,
        bool enableDebugJsonl,
        string runId)
    {
        var clusterConfig = new
        {
            ClusterId,
            Default = true,
            RepoType = dbEngine.ToString(),
            ConnectionString = masterConnectionString,
            TransientThreshold,
            DisabledPriorities,
            DebugJsonlFilePath = enableDebugJsonl ? DebugJsonlContainerPath : null,
            AgentConnections = allAgentConnections,
            Workers = workers
        };

        var clusterConfigsJson = JsonSerializer.Serialize(new[] { clusterConfig });

        return new WorkerContainerSpec
        {
            Name = containerName,
            DockerfilePath = "Benchmarks/JobMaster.Benchmarks.JobMasterHost/Dockerfile",
            EnvironmentVariables = new Dictionary<string, string>
            {
                ["JOBMASTER_CLUSTER_CONFIGS_JSON"] = clusterConfigsJson,
                ["REDIS_CONNECTION_STRING"] = $"{BenchmarkContainerEnvironment.RedisNetworkAlias}:6379",
                ["BENCHMARK_RUN_ID"] = runId
            }
        };
    }

    // Maps an agent connection name to its dedicated database name -- kept in one place so
    // AllDatabaseNames (provisioning) and BuildWorkerSpecs (connection strings) can never drift apart.
    private static string DatabaseNameFor(string agentConnectionName) => $"benchmark_{agentConnectionName.Replace('-', '_')}";

    /// <summary>Every distinct database name across the cluster -- the master database plus a
    /// dedicated agent-connection database for the drainer and each executor (coordinator-only
    /// containers need none of their own, see the class remarks) -- so the caller can create them
    /// all up front (each SQL engine requires the database to exist before the app connects). When
    /// <paramref name="useNats"/> is true, only the master database needs provisioning: NATS
    /// JetStream has no CREATE DATABASE-equivalent, it self-provisions streams at startup. When
    /// <paramref name="sharedAgentConnection"/> is true, only one agent database is needed regardless
    /// of executor count.</summary>
    public static IReadOnlyList<string> AllDatabaseNames(bool useNats, int executorCount, bool sharedAgentConnection = false)
    {
        var names = new List<string> { BenchmarkContainerEnvironment.DbDatabaseName };
        if (useNats)
        {
            return names;
        }

        if (sharedAgentConnection)
        {
            names.Add("benchmark_agent");
            return names;
        }

        names.Add(DatabaseNameFor(DrainerAgentConnectionName));
        for (var i = 0; i < executorCount; i++)
        {
            names.Add(DatabaseNameFor($"agent-exec-{i}"));
        }

        return names;
    }

    private static string NatsConnectionString() =>
        $"nats://{BenchmarkContainerEnvironment.NatsUsername}:{BenchmarkContainerEnvironment.NatsPassword}@{BenchmarkContainerEnvironment.NatsNetworkAlias}:{BenchmarkContainerEnvironment.NatsPort}";

    // Flags (Max Pool Size, UseAffectedRows/AllowUserVariables) match exactly what
    // Tests/JobMaster.ScenarioTests' SqlServerPure/MySqlPure JSON templates already use -- JobMaster's
    // MySql provider specifically depends on UseAffectedRows/AllowUserVariables for its bulk
    // upsert logic.
    private static string BuildConnectionString(DbEngine dbEngine, string databaseName) => dbEngine switch
    {
        DbEngine.RavenDB =>
            $"Urls=http://{BenchmarkContainerEnvironment.DbNetworkAlias}:8080;Database={databaseName}",

        // Connect Timeout raised to 60s (from SqlClient's 15s default) -- real observed failure was
        // "Connection Timeout Expired" during the pre-login handshake under concurrent connection
        // load, not a rejection or a query-execution timeout. Max Pool Size deliberately left at 300
        // per explicit user instruction. Note: unlike Postgres/MySql, Microsoft.Data.SqlClient has no
        // connection-string-level command (query-execution) timeout -- that's only settable per
        // SqlCommand in code, so it isn't adjustable here without touching JobMaster.SqlBase/
        // JobMaster.SqlServer's Dapper call sites directly.
        DbEngine.SqlServer =>
            $"Server={BenchmarkContainerEnvironment.DbNetworkAlias};" +
            $"Database={databaseName};" +
            $"User Id={BenchmarkContainerEnvironment.SqlServerUsername};" +
            $"Password={BenchmarkContainerEnvironment.SqlServerPassword};" +
            "TrustServerCertificate=True;Max Pool Size=300;Connect Timeout=60;",

        DbEngine.MySql =>
            $"Server={BenchmarkContainerEnvironment.DbNetworkAlias};" +
            $"Port={BenchmarkContainerEnvironment.MySqlPort};" +
            $"Database={databaseName};" +
            $"User ID={BenchmarkContainerEnvironment.MySqlUsername};" +
            $"Password={BenchmarkContainerEnvironment.MySqlPassword};" +
            "UseAffectedRows=True;AllowUserVariables=True",

        // Pool size capped at 5 (down from 25) -- with up to ~20 containers each holding their own
        // master-DB pool (plus, in Pure mode, a second pool to their own agent database), a
        // per-container ceiling in the dozens multiplies into real, observed max_connections
        // exhaustion ("sorry, too many clients already") regardless of the server-side
        // max_connections setting. 5 keeps per-container demand small enough that this shouldn't
        // recur at any executor count this benchmark uses.
        DbEngine.Postgres =>
            $"Host={BenchmarkContainerEnvironment.DbNetworkAlias};" +
            $"Port={BenchmarkContainerEnvironment.DbPort};" +
            $"Database={databaseName};" +
            $"Username={BenchmarkContainerEnvironment.DbUsername};" +
            $"Password={BenchmarkContainerEnvironment.DbPassword};" +
            "Maximum Pool Size=5;Timeout=300;",
        _ => throw new ArgumentException("Invalid Db Engine")
    };
}
