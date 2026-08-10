using System.Text.Json;
using JobMaster.Benchmarks.Common.Containers;

namespace JobMaster.Benchmarks.Runner;

/// <summary>
/// Builds one JobMaster cluster-config JSON per worker container: the first container is always a
/// dedicated Coordinator+Drain (two Coordinator instances + one Drain instance), every other
/// container is Execution-only.
///
/// Every container's config declares ALL agent connections in the cluster, not just its own --
/// the coordinator needs to recognize every executor's <c>AgentConnectionId</c> to select their
/// buckets (<c>MasterBucketService.FilterApplicableBuckets</c> rejects any bucket whose connection
/// isn't in the calling process's own <c>ClusterConnConfig</c>).
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

    /// <summary>In-container path for the coordinator+drainer's optional debug JSONL mirror log --
    /// copied out to container-logs/ after the run when enabled. See <see cref="BuildWorkerSpecs"/>'s
    /// <c>enableDebugJsonl</c> parameter.</summary>
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
        int workerCount,
        string runId,
        int bucketsPerWorker = 1,
        int? bucketBufferSize = null,
        bool skipWarmUpTime = false,
        bool sharedAgentConnection = false,
        int coordinatorCount = 4,
        int? transferBatchSize = null,
        bool enableDebugJsonl = false)
    {
        if (workerCount < 2)
        {
            throw new ArgumentException("workerCount must be at least 2 (1 dedicated coordinator+drainer + at least 1 execution container).", nameof(workerCount));
        }

        var specs = new List<WorkerContainerSpec>(workerCount);

        var masterConnectionString = BuildConnectionString(dbEngine, BenchmarkContainerEnvironment.DbDatabaseName);

        // Every container's config gets this same full list -- see the class remarks on why a
        // coordinator that only knows its own connection can never select an executor's bucket.
        // sharedAgentConnection collapses this down to exactly one entry, used by every drain/execution
        // worker -- buckets (not separate databases) are the real partition unit between workers, so
        // this is architecturally valid; it exists to isolate "database count" as its own benchmark
        // variable, separate from bucket count.
        var allAgentConnections = sharedAgentConnection
            ? new object[]
            {
                useNats
                    ? new { Name = "agent-shared", RepositoryType = "NatsJetStream", ConnectionString = NatsConnectionString() }
                    : new { Name = "agent-shared", RepositoryType = dbEngine.ToString(), ConnectionString = BuildConnectionString(dbEngine, "benchmark_agent") },
            }
            : useNats
                ? Enumerable.Range(0, workerCount)
                    .Select(i => new
                    {
                        Name = $"agent-{i}",
                        RepositoryType = "NatsJetStream",
                        ConnectionString = NatsConnectionString(),
                    })
                    .ToArray<object>()
                : Enumerable.Range(0, workerCount)
                    .Select(i => new
                    {
                        Name = $"agent-{i}",
                        RepositoryType = dbEngine.ToString(),
                        ConnectionString = BuildConnectionString(dbEngine, $"benchmark_agent_{i}"),
                    })
                    .ToArray<object>();

        for (var i = 0; i < workerCount; i++)
        {
            var isCoordinatorDrainer = i == 0;
            var containerName = $"worker-{i}";
            var agentConnectionName = sharedAgentConnection ? "agent-shared" : $"agent-{i}";

            List<object> workers = [];
            if (isCoordinatorDrainer)
            {
                // coordinatorCount Coordinator instances -- AssignJobsToBucketsRunner's imminent-path
                // lock is keyed by a shared 10-second wall-clock window (doesn't scale with instance
                // count), but the scan-plan path (delayed jobs) picks a random lock key from a range
                // that widens with CountActiveCoordinatorWorkersAsync(), so multiple instances
                // genuinely divide that work. transferBatchSize defaults to null (SDK's 1000 default);
                // raising it previously OOM-killed the DB and caused duplicate dispatches when the
                // claim lock expired mid-batch -- now that dispatch is bulked and partitioned with
                // per-partition failure isolation, this is being retested at 5000.
                for (var c = 1; c <= coordinatorCount; c++)
                {
                    workers.Add(new { WorkerName = $"coordinator-{c}", WorkerMode = "Coordinator", SkipWarmUpTime = skipWarmUpTime, TransferBatchSize = transferBatchSize });
                }
                workers.Add(new { WorkerName = "drainer", AgentConnectionName = agentConnectionName, WorkerMode = "Drain", SkipWarmUpTime = skipWarmUpTime });
            }
            else
            {
                var bucketQtyConfig = new Dictionary<string, int> { ["Medium"] = bucketsPerWorker };
                workers.Add(new { WorkerName = "executor", AgentConnectionName = agentConnectionName, WorkerMode = "Execution", BucketQtyConfig = bucketQtyConfig, BucketBufferSize = bucketBufferSize, SkipWarmUpTime = skipWarmUpTime });
            }

            var clusterConfig = new
            {
                ClusterId,
                Default = true,
                RepoType = dbEngine.ToString(),
                ConnectionString = masterConnectionString,
                TransientThreshold,
                DisabledPriorities,
                DebugJsonlFilePath = isCoordinatorDrainer && enableDebugJsonl ? DebugJsonlContainerPath : null,
                AgentConnections = allAgentConnections,
                Workers = workers
            };

            var clusterConfigsJson = JsonSerializer.Serialize(new[] { clusterConfig });

            specs.Add(new WorkerContainerSpec
            {
                Name = containerName,
                DockerfilePath = "Benchmarks/JobMaster.Benchmarks.JobMasterHost/Dockerfile",
                EnvironmentVariables = new Dictionary<string, string>
                {
                    ["JOBMASTER_CLUSTER_CONFIGS_JSON"] = clusterConfigsJson,
                    ["REDIS_CONNECTION_STRING"] = $"{BenchmarkContainerEnvironment.RedisNetworkAlias}:6379",
                    ["BENCHMARK_RUN_ID"] = runId
                }
            });
        }

        return specs;
    }

    /// <summary>Every distinct database name across every worker's config -- the master database
    /// plus each container's own dedicated agent-connection database -- so the caller can create
    /// them all up front (each SQL engine requires the database to exist before the app connects).
    /// When <paramref name="useNats"/> is true, only the master database needs provisioning: NATS
    /// JetStream has no CREATE DATABASE-equivalent, it self-provisions streams at startup. When
    /// <paramref name="sharedAgentConnection"/> is true, only one agent database is needed regardless
    /// of worker count.</summary>
    public static IReadOnlyList<string> AllDatabaseNames(bool useNats, int workerCount, bool sharedAgentConnection = false)
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

        for (var i = 0; i < workerCount; i++)
        {
            names.Add($"benchmark_agent_{i}");
        }

        return names;
    }

    private static string NatsConnectionString() =>
        $"nats://{BenchmarkContainerEnvironment.NatsUsername}:{BenchmarkContainerEnvironment.NatsPassword}@{BenchmarkContainerEnvironment.NatsNetworkAlias}:{BenchmarkContainerEnvironment.NatsPort}";

    // Flags (Max Pool Size, UseAffectedRows/AllowUserVariables) match exactly what
    // Tests/JobMaster.ScenarioTests' SqlServerPure/MySqlPure JSON templates already use -- JobMaster's
    // MySql provider specifically depends on UseAffectedRows/AllowUserVariables for its bulk
    // upsert logic, confirmed via those working templates rather than guessed.
    private static string BuildConnectionString(DbEngine dbEngine, string databaseName) => dbEngine switch
    {
        DbEngine.SqlServer =>
            $"Server={BenchmarkContainerEnvironment.DbNetworkAlias};" +
            $"Database={databaseName};" +
            $"User Id={BenchmarkContainerEnvironment.SqlServerUsername};" +
            $"Password={BenchmarkContainerEnvironment.SqlServerPassword};" +
            "TrustServerCertificate=True;Max Pool Size=300;",

        DbEngine.MySql =>
            $"Server={BenchmarkContainerEnvironment.DbNetworkAlias};" +
            $"Port={BenchmarkContainerEnvironment.MySqlPort};" +
            $"Database={databaseName};" +
            $"User ID={BenchmarkContainerEnvironment.MySqlUsername};" +
            $"Password={BenchmarkContainerEnvironment.MySqlPassword};" +
            "UseAffectedRows=True;AllowUserVariables=True",

        DbEngine.Postgres =>
            $"Host={BenchmarkContainerEnvironment.DbNetworkAlias};" +
            $"Port={BenchmarkContainerEnvironment.DbPort};" +
            $"Database={databaseName};" +
            $"Username={BenchmarkContainerEnvironment.DbUsername};" +
            $"Password={BenchmarkContainerEnvironment.DbPassword};" +
            "Maximum Pool Size=25;Timeout=60;",
        _ => throw new ArgumentException("Invalid Db Engine")
    };
}
