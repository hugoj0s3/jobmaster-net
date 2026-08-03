using JobMaster.Benchmarks.Common.Containers;

namespace JobMaster.Benchmarks.QuartzRunner;

/// <summary>
/// Builds N identical worker-container specs -- Quartz.NET's `AdoJobStore` clustering is genuinely
/// peer-to-peer (row-locking against the shared QRTZ_LOCKS/QRTZ_FIRED_TRIGGERS tables), with no
/// coordinator/drain concept, unlike JobMaster's bucket-assignment model. Every container gets
/// `quartz.scheduler.instanceId = AUTO` (set in QuartzHost's Program.cs, not here) to auto-generate
/// a unique per-process ID, so every container's config can be genuinely identical -- there's no
/// per-container role split to encode here at all.
/// </summary>
public static class QuartzTopologyBuilder
{
    public const string DatabaseName = "quartz";

    public static IReadOnlyList<WorkerContainerSpec> BuildWorkerSpecs(DbEngine dbEngine, int workerCount, string runId)
    {
        if (workerCount < 1)
        {
            throw new ArgumentException("workerCount must be at least 1.", nameof(workerCount));
        }

        var connectionString = BuildConnectionString(dbEngine);

        var specs = new List<WorkerContainerSpec>(workerCount);
        for (var i = 0; i < workerCount; i++)
        {
            specs.Add(new WorkerContainerSpec
            {
                Name = $"worker-{i}",
                DockerfilePath = "Benchmarks/JobMaster.Benchmarks.QuartzHost/Dockerfile",
                EnvironmentVariables = new Dictionary<string, string>
                {
                    ["QUARTZ_DB_ENGINE"] = dbEngine.ToString(),
                    ["QUARTZ_DB_CONNECTION_STRING"] = connectionString,
                    ["REDIS_CONNECTION_STRING"] = $"{BenchmarkContainerEnvironment.RedisNetworkAlias}:6379",
                    ["BENCHMARK_RUN_ID"] = runId
                }
            });
        }

        return specs;
    }

    private static string BuildConnectionString(DbEngine dbEngine) => dbEngine switch
    {
        // Max Pool Size=300 matches JobMasterTopologyBuilder's SqlServer connection string -- the
        // default pool size (100) was getting exhausted once /schedule-now started firing its batch
        // concurrently (Task.WhenAll) instead of one job at a time, surfacing as
        // Quartz.JobPersistenceException: Failed to obtain DB connection ... Timeout expired.
        DbEngine.SqlServer =>
            $"Server={BenchmarkContainerEnvironment.DbNetworkAlias},{BenchmarkContainerEnvironment.SqlServerPort};" +
            $"Database={DatabaseName};" +
            $"User Id={BenchmarkContainerEnvironment.SqlServerUsername};" +
            $"Password={BenchmarkContainerEnvironment.SqlServerPassword};" +
            "TrustServerCertificate=True;Max Pool Size=300;",

        DbEngine.MySql =>
            $"Server={BenchmarkContainerEnvironment.DbNetworkAlias};" +
            $"Port={BenchmarkContainerEnvironment.MySqlPort};" +
            $"Database={DatabaseName};" +
            $"User={BenchmarkContainerEnvironment.MySqlUsername};" +
            $"Password={BenchmarkContainerEnvironment.MySqlPassword};",

        _ =>
            $"Host={BenchmarkContainerEnvironment.DbNetworkAlias};" +
            $"Port={BenchmarkContainerEnvironment.DbPort};" +
            $"Database={DatabaseName};" +
            $"Username={BenchmarkContainerEnvironment.DbUsername};" +
            $"Password={BenchmarkContainerEnvironment.DbPassword};",
    };
}
