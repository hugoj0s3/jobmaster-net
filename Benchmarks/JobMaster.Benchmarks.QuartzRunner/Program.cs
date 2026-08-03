using Docker.DotNet;
using DotNet.Testcontainers.Containers;
using JobMaster.Benchmarks.Common.Cli;
using JobMaster.Benchmarks.Common.Containers;
using JobMaster.Benchmarks.Common.Load;
using JobMaster.Benchmarks.Common.Metrics;
using JobMaster.Benchmarks.Common.Recording;
using JobMaster.Benchmarks.Common.Reporting;
using JobMaster.Benchmarks.Common.Scheduling;
using JobMaster.Benchmarks.QuartzRunner;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using StackExchange.Redis;

// --db is Quartz-specific (Hangfire/JobMaster runners don't have a multi-engine choice today), so
// it's parsed here rather than added to the shared, framework-agnostic CliOptions.
var (dbEngine, remainingArgs) = ParseDbEngine(args);
var options = CliOptions.Parse(remainingArgs);

var dbLabel = dbEngine switch
{
    DbEngine.Postgres => "postgres-adojobstore",
    DbEngine.MySql => "mysql-adojobstore",
    _ => "sqlserver-adojobstore",
};
Console.WriteLine($"Framework=quartz Db={dbLabel} Rate={options.TargetJobsPerMinute}/min " +
                   $"Workers={options.WorkerCount} Duration={options.Duration} Smoke={options.Smoke}");
if (options.StepDownJobsPerMinute.HasValue)
{
    Console.WriteLine($"StepDown: -> {options.StepDownJobsPerMinute}/min at {options.StepDownAt}");
}

var runId = Guid.NewGuid().ToString("N")[..8];
Console.WriteLine($"RunId={runId}");

var workerSpecs = QuartzTopologyBuilder.BuildWorkerSpecs(dbEngine, options.WorkerCount, runId);

await using var environment = new BenchmarkContainerEnvironment();

Console.WriteLine("Starting containers (DB, Redis, workers)...");
await environment.StartAsync(
    workerSpecs,
    databaseNamesToProvision: [QuartzTopologyBuilder.DatabaseName],
    dbEngine: dbEngine,
    afterDbProvisionedAsync: async db =>
    {
        Console.WriteLine("Provisioning Quartz schema (QRTZ_* tables)...");
        var adminConnectionString = BuildScopedConnectionString(dbEngine, db.GetConnectionString(), QuartzTopologyBuilder.DatabaseName);
        await QuartzSchemaProvisioner.CreateSchemaAsync(dbEngine, adminConnectionString);
        Console.WriteLine("Quartz schema ready.");
    },
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

var allContainers = new List<(string Name, string Id)> { (BenchmarkContainerEnvironment.DbNetworkAlias, ((IContainer)environment.DbContainer).Id) };
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
    BurstBatchSize = options.BurstBatchSize
};
var loadGenerator = new LoadGenerator(roundRobinClient, latencyRecorder, loadGeneratorOptions, scheduleCallLatencyRecorder);
var isBurst = options.BurstTotalJobs.HasValue;

var startedAtUtc = DateTime.UtcNow;
Console.WriteLine(isBurst
    ? $"Burst load generation starting: {options.BurstTotalJobs} jobs in batches of {options.BurstBatchSize}..."
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

var dbConfigLabel = dbEngine switch
{
    DbEngine.Postgres => "PostgresAdoJobStore",
    DbEngine.MySql => "MySqlAdoJobStore",
    _ => "SqlServerAdoJobStore",
};
var outputPrefix = dbEngine switch
{
    DbEngine.Postgres => "quartz_postgres",
    DbEngine.MySql => "quartz_mysql",
    _ => "quartz_sqlserver",
};

var metadata = new BenchmarkRunMetadata(
    Framework: "Quartz.NET",
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
var outputDirectory = options.OutputDirectory ?? Path.Combine("Benchmarks", "Results", $"{outputPrefix}_{outputDirectorySuffix}");
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

// Captured before teardown so anomalies can actually be diagnosed from the Quartz host's own log
// output, not just guessed at from external symptoms.
Console.WriteLine("Dumping Quartz DB diagnostics (QRTZ_* row counts)...");
var diagnosticsDirectory = Path.Combine(outputDirectory, "db-diagnostics");
Directory.CreateDirectory(diagnosticsDirectory);

var quartzDbConnectionString = BuildScopedConnectionString(dbEngine, ((IDatabaseContainer)environment.DbContainer).GetConnectionString(), QuartzTopologyBuilder.DatabaseName);

await DumpQueryAsync(dbEngine, quartzDbConnectionString, "SELECT COUNT(*) AS cnt FROM QRTZ_JOB_DETAILS",
    Path.Combine(diagnosticsDirectory, "job-details-count.txt"));
await DumpQueryAsync(dbEngine, quartzDbConnectionString, "SELECT COUNT(*) AS cnt FROM QRTZ_TRIGGERS",
    Path.Combine(diagnosticsDirectory, "triggers-count.txt"));
await DumpQueryAsync(dbEngine, quartzDbConnectionString, "SELECT TRIGGER_STATE, COUNT(*) AS cnt FROM QRTZ_TRIGGERS GROUP BY TRIGGER_STATE",
    Path.Combine(diagnosticsDirectory, "triggers-by-state.txt"));
await DumpQueryAsync(dbEngine, quartzDbConnectionString, "SELECT * FROM QRTZ_FIRED_TRIGGERS",
    Path.Combine(diagnosticsDirectory, "fired-triggers.txt"));
await DumpQueryAsync(dbEngine, quartzDbConnectionString, "SELECT * FROM QRTZ_SCHEDULER_STATE",
    Path.Combine(diagnosticsDirectory, "scheduler-state.txt"));

Console.WriteLine($"DB diagnostics written to {diagnosticsDirectory}");

Console.WriteLine("Capturing container logs...");
var logsDirectory = Path.Combine(outputDirectory, "container-logs");
Directory.CreateDirectory(logsDirectory);

var (dbStdOut, dbStdErr) = await ((IContainer)environment.DbContainer).GetLogsAsync();
await File.WriteAllTextAsync(Path.Combine(logsDirectory, "db.stdout.log"), dbStdOut);
await File.WriteAllTextAsync(Path.Combine(logsDirectory, "db.stderr.log"), dbStdErr);

foreach (var (container, spec) in environment.WorkerContainers.Zip(workerSpecs))
{
    var (stdOut, stdErr) = await container.GetLogsAsync();
    await File.WriteAllTextAsync(Path.Combine(logsDirectory, $"{spec.Name}.stdout.log"), stdOut);
    await File.WriteAllTextAsync(Path.Combine(logsDirectory, $"{spec.Name}.stderr.log"), stdErr);
}

Console.WriteLine($"Container logs written to {logsDirectory}");

Console.WriteLine("Tearing down containers...");

return;

// db/db.GetType() name kept out of it deliberately -- the diagnostic SQL text is plain ANSI
// (COUNT/GROUP BY/SELECT *), which all three engines run unchanged.
static System.Data.Common.DbConnection CreateConnection(DbEngine dbEngine, string connectionString) => dbEngine switch
{
    DbEngine.Postgres => new NpgsqlConnection(connectionString),
    DbEngine.MySql => new MySqlConnection(connectionString),
    _ => new SqlConnection(connectionString),
};

static (DbEngine DbEngine, string[] RemainingArgs) ParseDbEngine(string[] args)
{
    var dbEngine = DbEngine.SqlServer; // preserves existing default behavior
    var remaining = new List<string>(args.Length);

    for (var i = 0; i < args.Length; i++)
    {
        if (args[i] == "--db")
        {
            dbEngine = args[++i].ToLowerInvariant() switch
            {
                "postgres" => DbEngine.Postgres,
                "mysql" => DbEngine.MySql,
                "sqlserver" => DbEngine.SqlServer,
                var other => throw new ArgumentException($"Unknown --db value '{other}'. Expected postgres, mysql, or sqlserver."),
            };
        }
        else
        {
            remaining.Add(args[i]);
        }
    }

    return (dbEngine, remaining.ToArray());
}

// Scopes an admin/root connection string down to the benchmark's own database -- each engine's
// connection string syntax differs (Initial Catalog vs. Database=), so this can't be one shared
// ConnectionStringBuilder type.
static string BuildScopedConnectionString(DbEngine dbEngine, string adminConnectionString, string databaseName) => dbEngine switch
{
    DbEngine.Postgres => new NpgsqlConnectionStringBuilder(adminConnectionString) { Database = databaseName }.ConnectionString,
    DbEngine.MySql => new MySqlConnectionStringBuilder(adminConnectionString) { Database = databaseName }.ConnectionString,
    _ => new SqlConnectionStringBuilder(adminConnectionString) { InitialCatalog = databaseName }.ConnectionString,
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
