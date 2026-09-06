using JobMaster.Benchmarks.Common.Containers;

namespace JobMaster.Benchmarks.HangfireRunner;

/// <summary>
/// Builds N identical worker-container specs -- Hangfire's SQL Server storage clustering is
/// genuinely peer-to-peer (row-level locking against the shared HangFire.JobQueue table), no
/// coordinator/drain concept, same model as Quartz.NET's clustered AdoJobStore. Every container
/// gets a unique <c>ServerName</c> (set in HangfireHost's Program.cs, not here), so every
/// container's config can be genuinely identical.
/// </summary>
public static class HangfireTopologyBuilder
{
    public const string DatabaseName = "hangfire";

    public static IReadOnlyList<WorkerContainerSpec> BuildWorkerSpecs(int workerCount, string runId, int jobConcurrency)
    {
        if (workerCount < 1)
        {
            throw new ArgumentException("workerCount must be at least 1.", nameof(workerCount));
        }

        // Max Pool Size=300 matches JobMasterTopologyBuilder's SqlServer connection string --
        // QuartzTopologyBuilder hit real connection-pool exhaustion at the default (100) once its
        // /schedule-now endpoint started firing batches concurrently instead of sequentially;
        // applying the same fix here preemptively since HangfireHost's endpoint changed the same way.
        var sqlServerConnectionString =
            $"Server={BenchmarkContainerEnvironment.DbNetworkAlias},{BenchmarkContainerEnvironment.SqlServerPort};" +
            $"Database={DatabaseName};" +
            $"User Id={BenchmarkContainerEnvironment.SqlServerUsername};" +
            $"Password={BenchmarkContainerEnvironment.SqlServerPassword};" +
            "TrustServerCertificate=True;Max Pool Size=300;Connect Timeout=300;Command Timeout=120;";

        var specs = new List<WorkerContainerSpec>(workerCount);
        for (var i = 0; i < workerCount; i++)
        {
            specs.Add(new WorkerContainerSpec
            {
                Name = $"worker-{i}",
                DockerfilePath = "Benchmarks/JobMaster.Benchmarks.HangfireHost/Dockerfile",
                EnvironmentVariables = new Dictionary<string, string>
                {
                    ["HANGFIRE_SQLSERVER_CONNECTION_STRING"] = sqlServerConnectionString,
                    ["REDIS_CONNECTION_STRING"] = $"{BenchmarkContainerEnvironment.RedisNetworkAlias}:6379",
                    ["BENCHMARK_RUN_ID"] = runId,
                    ["HANGFIRE_WORKER_COUNT"] = jobConcurrency.ToString()
                }
            });
        }

        return specs;
    }
}
