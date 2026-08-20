using Docker.DotNet;
using DotNet.Testcontainers.Containers;
using JobMaster.Benchmarks.Common.Containers;
using JobMaster.Benchmarks.Common.Load;
using JobMaster.Benchmarks.Common.Metrics;
using JobMaster.Benchmarks.Common.Recording;
using JobMaster.Benchmarks.Common.Reporting;
using JobMaster.Benchmarks.Common.Scheduling;
using JobMaster.Benchmarks.Runner;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using StackExchange.Redis;

var options = CliOptions.Parse(args);
var dbLabel = (options.DbEngine, options.UseNats) switch
{
    (DbEngine.SqlServer, true) => "sqlservernats",
    (DbEngine.MySql, true) => "mysqlnats",
    (DbEngine.Postgres, true) => "postgresnats",
    (DbEngine.SqlServer, false) => "sqlserverpure",
    (DbEngine.MySql, false) => "mysqlpure",
    (DbEngine.RavenDB, false) => "ravendbpure",
    _ => "postgrespure",
};
Console.WriteLine($"Framework=jobmaster Db={dbLabel} Rate={options.TargetJobsPerMinute}/min " +
                   $"Workers={options.WorkerCount} BucketsPerWorker={options.BucketsPerWorker} " +
                   $"BucketBufferSize={options.BucketBufferSize?.ToString() ?? "default"} SkipWarmUpTime={options.SkipWarmUpTime} " +
                   $"CoordinatorCount={options.CoordinatorCount} TransferBatchSize={options.TransferBatchSize?.ToString() ?? "default"} " +
                   $"Duration={options.Duration} Smoke={options.Smoke}");
if (options.StepDownJobsPerMinute.HasValue)
{
    Console.WriteLine($"StepDown: -> {options.StepDownJobsPerMinute}/min at {options.StepDownAt}");
}

var runId = Guid.NewGuid().ToString("N")[..8];
Console.WriteLine($"RunId={runId}");

var workerSpecs = JobMasterTopologyBuilder.BuildWorkerSpecs(options.DbEngine, options.UseNats, options.WorkerCount, runId, options.BucketsPerWorker, options.BucketBufferSize, options.SkipWarmUpTime, options.SharedAgentConnection, options.CoordinatorCount, options.TransferBatchSize, options.EnableDebugJsonl);
var databaseNames = JobMasterTopologyBuilder.AllDatabaseNames(options.UseNats, options.WorkerCount, options.SharedAgentConnection);

await using var environment = new BenchmarkContainerEnvironment();

Console.WriteLine("Starting containers (DB, Redis, workers)...");
await environment.StartAsync(
    workerSpecs,
    databaseNames,
    dbEngine: options.DbEngine,
    includeNats: options.UseNats,
    dbNanoCpus: (long)(options.DbCpu * 1_000_000_000),
    dbMemoryBytes: (long)(options.DbMemoryGb * 1024 * 1024 * 1024));
Console.WriteLine("Containers ready.");

if (options.WarmupDelay > TimeSpan.Zero)
{
    Console.WriteLine($"Warmup delay: waiting {options.WarmupDelay} before starting load generation...");
    await Task.Delay(options.WarmupDelay);
}

var redisConnectionString = $"{environment.RedisContainer.Hostname}:{environment.RedisContainer.GetMappedPublicPort(6379)}";
var mux = await ConnectionMultiplexer.ConnectAsync(redisConnectionString);

// Timeout raised from HttpClient's 100s default -- a large burst batch can take longer than that
// to schedule; must match across all three runners for a fair comparison.
var scheduleClients = Enumerable.Range(0, workerSpecs.Count)
    .Select(i => (IScheduleClient)new HttpScheduleClient(new HttpClient { BaseAddress = new Uri(environment.GetWorkerBaseUrl(i)), Timeout = TimeSpan.FromMinutes(60) }))
    .ToList();
var roundRobinClient = new RoundRobinScheduleClient(scheduleClients);

var latencyRecorder = new RedisLatencyRecorder(mux, runId);
var scheduleCallLatencyRecorder = new ScheduleCallLatencyRecorder();

using var dockerClient = new DockerClientConfiguration().CreateClient();
var statsSampler = new ContainerStatsSampler(dockerClient);

var allContainers = new List<(string Name, string Id)> { (BenchmarkContainerEnvironment.DbNetworkAlias, environment.DbContainerForOps.Id) };
allContainers.AddRange(environment.WorkerContainers.Select((c, i) => (workerSpecs[i].Name, c.Id)));

var statsSamples = new List<ContainerStatsSample>();
var healthSamples = new List<ContainerHealthSample>();
using var samplingCts = new CancellationTokenSource();

var samplingTask = Task.Run(async () =>
{
    var tick = 0;
    while (!samplingCts.IsCancellationRequested)
    {
        foreach (var (name, id) in allContainers)
        {
            try
            {
                var stats = await statsSampler.SampleStatsAsync(id, name, samplingCts.Token);
                if (stats is not null) statsSamples.Add(stats);

                if (tick % 6 == 0) // health/inspect roughly every 6th stats tick (~30-60s)
                {
                    healthSamples.Add(await statsSampler.SampleHealthAsync(id, name, samplingCts.Token));
                }
            }
            catch (OperationCanceledException)
            {
                // Sampling loop is stopping.
            }
        }

        tick++;
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(5), samplingCts.Token);
        }
        catch (OperationCanceledException)
        {
            break;
        }
    }
});

var loadGeneratorOptions = new LoadGeneratorOptions
{
    TargetJobsPerMinute = options.TargetJobsPerMinute,
    Duration = options.Duration,
    DelayMin = options.DelayMin,
    DelayMax = options.DelayMax,
    StepDownJobsPerMinute = options.StepDownJobsPerMinute,
    StepDownAt = options.StepDownAt,
    MaxConcurrentRequests = options.MaxConcurrentRequests ?? 200,
    BurstTotalJobs = options.BurstTotalJobs,
    BurstBatchSize = options.BurstBatchSize,
    BurstDelay = options.BurstDelay
};
var loadGenerator = new LoadGenerator(roundRobinClient, latencyRecorder, loadGeneratorOptions, scheduleCallLatencyRecorder);
var isBurst = options.BurstTotalJobs.HasValue;

var startedAtUtc = DateTime.UtcNow;
Console.WriteLine(isBurst
    ? $"Burst load generation starting: {options.BurstTotalJobs} jobs in batches of {options.BurstBatchSize}" +
      (options.BurstDelay is { } burstDelay ? $" (all delayed by {burstDelay})..." : " (immediate)...")
    : $"Load generation starting for {options.Duration}...");
try
{
    await loadGenerator.RunAsync();
    Console.WriteLine(isBurst
        ? "All batches fired. Polling for completions..."
        : "Load generation finished. Waiting grace period for in-flight delayed jobs...");
}
catch (Exception ex)
{
    // Keep going into the grace/diagnostics/log-capture steps below even on failure -- otherwise a
    // single failed schedule request tears everything down before the container logs (the whole
    // point of capturing them) are ever written.
    Console.WriteLine($"Load generation failed: {ex}");
}

IReadOnlyList<CompletionSample> completionTimeline;
if (isBurst)
{
    // No fixed grace period makes sense here -- every job is immediate, so instead poll until the
    // burst fully drains or BurstMaxWait elapses, whichever comes first. Wait on the ACTUAL count of
    // jobs recorded as scheduled, not the configured --burst-total -- if some schedule calls failed
    // (e.g. a host-side error on a large batch), those jobs never got created, so waiting for the
    // originally-requested total would spin until BurstMaxWait every time, even though everything
    // that could complete already did.
    var totalActuallyScheduled = (int)await mux.GetDatabase().HashLengthAsync($"bench:{runId}:expected");
    var (waited, timeline) = await BurstCompletionWaiter.WaitAsync(mux, runId, totalActuallyScheduled, options.BurstMaxWait, TimeSpan.FromSeconds(2));
    completionTimeline = timeline;
    Console.WriteLine($"Burst drain wait finished after {waited} (cap was {options.BurstMaxWait}).");
}
else
{
    // Poll for actual completion instead of sleeping a fixed guessed duration -- finishes as soon
    // as every scheduled job completes (no wasted wall-clock time), and doubles as the "how long did
    // it take everything to actually finish" measurement. --grace-minutes now controls the safety-net
    // timeout (default 2 hours) rather than a fixed sleep length.
    var totalScheduled = (int)await mux.GetDatabase().HashLengthAsync($"bench:{runId}:expected");
    var maxWait = options.GraceOverride ?? TimeSpan.FromHours(2);
    Console.WriteLine($"Waiting for all {totalScheduled} scheduled jobs to complete (cap {maxWait})...");
    var (drainWaited, timeline) = await BurstCompletionWaiter.WaitAsync(mux, runId, totalScheduled, maxWait, TimeSpan.FromSeconds(5));
    completionTimeline = timeline;
    Console.WriteLine($"Drain wait finished after {drainWaited} (cap was {maxWait}).");
}

samplingCts.Cancel();
try { await samplingTask; } catch (OperationCanceledException) { }

var completedAtUtc = DateTime.UtcNow;

Console.WriteLine("Computing latency/correctness report...");
var joiner = new LatencyJoiner(mux, runId);
var latencyReport = await joiner.ComputeAsync();

var dbConfigLabel = (options.DbEngine, options.UseNats) switch
{
    (DbEngine.SqlServer, true) => "SqlServerNats",
    (DbEngine.MySql, true) => "MySqlNats",
    (DbEngine.Postgres, true) => "PostgresNats",
    (DbEngine.SqlServer, false) => "SqlServerPure",
    (DbEngine.MySql, false) => "MySqlPure",
    (DbEngine.RavenDB, false) => "RavenDbPure",
    _ => "PostgresPure",
};

var metadata = new BenchmarkRunMetadata(
    Framework: "JobMaster",
    DbConfig: dbConfigLabel,
    TargetJobsPerMinute: options.TargetJobsPerMinute,
    WorkerCount: options.WorkerCount,
    Duration: options.Duration,
    StartedAtUtc: startedAtUtc,
    CompletedAtUtc: completedAtUtc,
    TestType: isBurst ? "Burst" : "Paced",
    TotalBurstJobs: options.BurstTotalJobs,
    FailedRequestCount: loadGenerator.FailedRequestCount);

var scheduleCallLatencyReport = scheduleCallLatencyRecorder.Compute();

var outputDirectorySuffix = isBurst ? $"burst{options.BurstTotalJobs}_{runId}" : runId;
var outputDirectory = options.OutputDirectory ?? Path.Combine("Benchmarks", "Results", $"jobmaster_{dbLabel}_{outputDirectorySuffix}");
var reportWriter = new BenchmarkReportWriter();
await reportWriter.WriteAsync(outputDirectory, metadata, latencyReport, scheduleCallLatencyReport, statsSamples, healthSamples, completionTimeline);

Console.WriteLine($"Report written to {outputDirectory}");
var elapsedSeconds = isBurst ? (completedAtUtc - startedAtUtc).TotalSeconds : options.Duration.TotalSeconds;
Console.WriteLine($"Scheduled={latencyReport.TotalScheduled} ({latencyReport.TotalScheduled / elapsedSeconds:F2}/sec) " +
                   $"Completed={latencyReport.TotalCompletedJobs} ({latencyReport.TotalCompletedJobs / elapsedSeconds:F2}/sec) " +
                   $"Lost={latencyReport.LostCount} Duplicated={latencyReport.DuplicatedCount}");
Console.WriteLine($"Immediate mean/p50/p90/p99={latencyReport.Immediate.MeanMs}/{latencyReport.Immediate.P50Ms}/{latencyReport.Immediate.P90Ms}/{latencyReport.Immediate.P99Ms}ms");
Console.WriteLine($"Delayed   mean/p50/p90/p99={latencyReport.Delayed.MeanMs}/{latencyReport.Delayed.P50Ms}/{latencyReport.Delayed.P90Ms}/{latencyReport.Delayed.P99Ms}ms");
Console.WriteLine($"ScheduleCall(now)   mean/p50/p90/p99={scheduleCallLatencyReport.Immediate.MeanMs:F1}/{scheduleCallLatencyReport.Immediate.P50Ms:F1}/{scheduleCallLatencyReport.Immediate.P90Ms:F1}/{scheduleCallLatencyReport.Immediate.P99Ms:F1}ms");
Console.WriteLine($"ScheduleCall(after) mean/p50/p90/p99={scheduleCallLatencyReport.Delayed.MeanMs:F1}/{scheduleCallLatencyReport.Delayed.P50Ms:F1}/{scheduleCallLatencyReport.Delayed.P90Ms:F1}/{scheduleCallLatencyReport.Delayed.P99Ms:F1}ms");

// Captured before teardown so anomalies (like the lost/high-latency jobs above) can actually be
// diagnosed from the JobMaster host's own log output, not just guessed at from external symptoms.
// SQL-only: these are raw ADO queries against JobMaster's SQL schema (jm_job/jm_bucket/jm_log/
// jm_message_dispatcher), which has no RavenDB equivalent (RavenDB uses collections + RQL, not this
// schema at all) -- not attempted for RavenDB rather than half-built with no real value.
var diagnosticsDirectory = Path.Combine(outputDirectory, "db-diagnostics");
Directory.CreateDirectory(diagnosticsDirectory);

if (options.DbEngine == DbEngine.RavenDB)
{
    Console.WriteLine("Skipping DB diagnostics dump -- not applicable for RavenDB (SQL-schema-specific).");
}
else
{
    Console.WriteLine("Dumping master DB diagnostics (jm_job status distribution, jm_log entries)...");
    var masterConnectionString = BuildScopedConnectionString(options.DbEngine, environment.DbContainer.GetConnectionString(), BenchmarkContainerEnvironment.DbDatabaseName);

    await DumpQueryAsync(options.DbEngine, masterConnectionString, "SELECT status, COUNT(*) AS cnt FROM jm_job GROUP BY status ORDER BY status",
        Path.Combine(diagnosticsDirectory, "job-status-distribution.txt"));
    await DumpQueryAsync(options.DbEngine, masterConnectionString, "SELECT * FROM jm_bucket ORDER BY id",
        Path.Combine(diagnosticsDirectory, "buckets.txt"));
    await DumpQueryAsync(options.DbEngine, masterConnectionString, "SELECT * FROM jm_log ORDER BY timestamp_utc ASC",
        Path.Combine(diagnosticsDirectory, "jm-log.txt"));
    await DumpQueryAsync(options.DbEngine, masterConnectionString, "SELECT job_id, bucket_id, started_at, finalized_at, DATEDIFF(millisecond, started_at, finalized_at) AS duration_ms FROM JM_job_execution ORDER BY started_at ASC",
        Path.Combine(diagnosticsDirectory, "job-execution.txt"));

    // The processing message queue (jm_message_dispatcher) lives in each worker's own dedicated agent
    // connection database, not the master database -- same DB server, different database name. Only
    // applies to SQL-backed agent connections: NATS agent connections have no such table (JetStream
    // tracks its own state), so skip this when --nats is set.
    if (!options.UseNats)
    {
        var agentDatabaseName = options.SharedAgentConnection ? "benchmark_agent" : "benchmark_agent_0";
        var agentConnectionString = BuildScopedConnectionString(options.DbEngine, environment.DbContainer.GetConnectionString(), agentDatabaseName);
        await DumpQueryAsync(options.DbEngine, agentConnectionString, "SELECT COUNT(*) AS cnt FROM jm_message_dispatcher",
            Path.Combine(diagnosticsDirectory, "agent0-message-dispatcher-count.txt"));

        // TOP/LIMIT syntax differs per engine -- SQL Server doesn't support LIMIT.
        var sampleQuery = options.DbEngine == DbEngine.SqlServer
            ? "SELECT TOP 50 * FROM jm_message_dispatcher ORDER BY reference_time ASC"
            : "SELECT * FROM jm_message_dispatcher ORDER BY reference_time ASC LIMIT 50";
        await DumpQueryAsync(options.DbEngine, agentConnectionString, sampleQuery,
            Path.Combine(diagnosticsDirectory, "agent0-message-dispatcher-sample.txt"));
    }

    Console.WriteLine($"DB diagnostics written to {diagnosticsDirectory}");
}

Console.WriteLine("Capturing container logs...");
var logsDirectory = Path.Combine(outputDirectory, "container-logs");
Directory.CreateDirectory(logsDirectory);

var (dbStdOut, dbStdErr) = await environment.DbContainerForOps.GetLogsAsync();
await File.WriteAllTextAsync(Path.Combine(logsDirectory, "db.stdout.log"), dbStdOut);
await File.WriteAllTextAsync(Path.Combine(logsDirectory, "db.stderr.log"), dbStdErr);

foreach (var (container, spec) in environment.WorkerContainers.Zip(workerSpecs))
{
    var (stdOut, stdErr) = await container.GetLogsAsync();
    await File.WriteAllTextAsync(Path.Combine(logsDirectory, $"{spec.Name}.stdout.log"), stdOut);
    await File.WriteAllTextAsync(Path.Combine(logsDirectory, $"{spec.Name}.stderr.log"), stdErr);
}

if (options.EnableDebugJsonl)
{
    // Only worker-0 (the coordinator+drainer container) is configured with a DebugJsonlFilePath --
    // see JobMasterTopologyBuilder.BuildWorkerSpecs. Debug-level logs (e.g. runner tick timing) are
    // never persisted to jm_log, so this file is the only way to recover them after the run.
    // JsonlFileLogger doesn't write to the literal path -- it chunks output into 4-hour buckets named
    // "{baseName}_{yyyyMMdd_HH}.jsonl", so a run spanning a chunk boundary produces multiple files.
    var coordinatorContainer = environment.WorkerContainers.Zip(workerSpecs)
        .FirstOrDefault(x => x.Second.Name == "worker-0").First;
    if (coordinatorContainer != null)
    {
        try
        {
            var containerDir = Path.GetDirectoryName(JobMasterTopologyBuilder.DebugJsonlContainerPath)!.Replace('\\', '/');
            var baseName = Path.GetFileNameWithoutExtension(JobMasterTopologyBuilder.DebugJsonlContainerPath);
            var listResult = await coordinatorContainer.ExecAsync(["sh", "-c", $"ls {containerDir}/{baseName}_*.jsonl 2>/dev/null"]);
            var chunkPaths = listResult.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            foreach (var chunkPath in chunkPaths)
            {
                var chunkBytes = await coordinatorContainer.ReadFileAsync(chunkPath);
                await File.WriteAllBytesAsync(Path.Combine(logsDirectory, Path.GetFileName(chunkPath)), chunkBytes);
            }

            if (chunkPaths.Length == 0)
            {
                Console.WriteLine("No debug JSONL chunk files found on coordinator container.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Could not read debug JSONL from coordinator container: {ex.Message}");
        }
    }
}

Console.WriteLine($"Container logs written to {logsDirectory}");

Console.WriteLine("Tearing down containers...");

return;

static System.Data.Common.DbConnection CreateConnection(DbEngine dbEngine, string connectionString) => dbEngine switch
{
    DbEngine.SqlServer => new SqlConnection(connectionString),
    DbEngine.MySql => new MySqlConnection(connectionString),
    _ => new NpgsqlConnection(connectionString),
};

// Scopes the DB container's own admin/root connection string down to a specific database --
// each engine's connection string syntax differs (Initial Catalog vs. Database=), so this can't
// be one shared ConnectionStringBuilder type.
static string BuildScopedConnectionString(DbEngine dbEngine, string adminConnectionString, string databaseName) => dbEngine switch
{
    DbEngine.SqlServer => new SqlConnectionStringBuilder(adminConnectionString) { InitialCatalog = databaseName }.ConnectionString,
    DbEngine.MySql => new MySqlConnectionStringBuilder(adminConnectionString) { Database = databaseName }.ConnectionString,
    _ => new NpgsqlConnectionStringBuilder(adminConnectionString) { Database = databaseName }.ConnectionString,
};

// Generic diagnostic dump -- doesn't assume exact column names/schema, just prints whatever
// columns exist so it survives schema differences without needing updates.
static async Task DumpQueryAsync(DbEngine dbEngine, string connectionString, string sqlText, string outputPath)
{
    try
    {
        await using var connection = CreateConnection(dbEngine, connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sqlText;
        await using var reader = await command.ExecuteReaderAsync();

        var sb = new System.Text.StringBuilder();
        var columnNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
        sb.AppendLine(string.Join(" | ", columnNames));

        var rowCount = 0;
        while (await reader.ReadAsync())
        {
            var values = Enumerable.Range(0, reader.FieldCount).Select(i => reader.IsDBNull(i) ? "" : reader.GetValue(i).ToString());
            sb.AppendLine(string.Join(" | ", values));
            rowCount++;
        }

        sb.AppendLine($"-- {rowCount} row(s)");
        await File.WriteAllTextAsync(outputPath, sb.ToString());
    }
    catch (Exception ex)
    {
        await File.WriteAllTextAsync(outputPath, $"Query failed: {ex}");
    }
}
