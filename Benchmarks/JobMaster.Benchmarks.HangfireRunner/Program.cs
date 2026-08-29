using Docker.DotNet;
using DotNet.Testcontainers.Containers;
using JobMaster.Benchmarks.Common.Cli;
using JobMaster.Benchmarks.Common.Containers;
using JobMaster.Benchmarks.Common.Load;
using JobMaster.Benchmarks.Common.Metrics;
using JobMaster.Benchmarks.Common.Recording;
using JobMaster.Benchmarks.Common.Reporting;
using JobMaster.Benchmarks.Common.Scheduling;
using JobMaster.Benchmarks.HangfireRunner;
using Microsoft.Data.SqlClient;
using StackExchange.Redis;

var options = CliOptions.Parse(args);
Console.WriteLine($"Framework=hangfire Db=sqlserver-storage Rate={options.TargetJobsPerMinute}/min " +
                   $"Workers={options.WorkerCount} Duration={options.Duration} Smoke={options.Smoke}");
if (options.StepDownJobsPerMinute.HasValue)
{
    Console.WriteLine($"StepDown: -> {options.StepDownJobsPerMinute}/min at {options.StepDownAt}");
}

var runId = Guid.NewGuid().ToString("N")[..8];
Console.WriteLine($"RunId={runId}");

var workerSpecs = HangfireTopologyBuilder.BuildWorkerSpecs(options.WorkerCount, runId);

await using var environment = new BenchmarkContainerEnvironment();

Console.WriteLine("Starting containers (DB, Redis, workers)...");
// No schema-provisioner hook needed -- unlike Quartz's AdoJobStore, Hangfire.SqlServer
// self-migrates its schema (PrepareSchemaIfNecessary defaults to true) from inside the host
// process on first connect. Database creation alone (via databaseNamesToProvision) is enough.
await environment.StartAsync(
    workerSpecs,
    databaseNamesToProvision: [HangfireTopologyBuilder.DatabaseName],
    dbEngine: DbEngine.SqlServer,
    dbNanoCpus: (long)(options.DbCpu * 1_000_000_000),
    dbMemoryBytes: (long)(options.DbMemoryGb * 1024 * 1024 * 1024),
    workerNanoCpus: (long)(options.WorkerCpu * 1_000_000_000),
    workerMemoryBytes: (long)(options.WorkerMemoryGb * 1024 * 1024 * 1024));
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
    BurstRequestsPerWorker = options.BurstRequestsPerWorker
};
var loadGenerator = new LoadGenerator(roundRobinClient, scheduleClients, latencyRecorder, loadGeneratorOptions, scheduleCallLatencyRecorder);
var isBurst = options.BurstTotalJobs.HasValue;

var startedAtUtc = DateTime.UtcNow;
Console.WriteLine(isBurst
    ? $"Burst load generation starting: {options.BurstTotalJobs} jobs across {workerSpecs.Count} workers x {options.BurstRequestsPerWorker} requests/worker..."
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
    Console.WriteLine($"Load generation failed: {ex}");
}

IReadOnlyList<CompletionSample> completionTimeline;
if (isBurst)
{
    var totalActuallyScheduled = (int)await mux.GetDatabase().HashLengthAsync($"bench:{runId}:expected");
    var (waited, timeline) = await BurstCompletionWaiter.WaitAsync(mux, runId, totalActuallyScheduled, options.BurstMaxWait, TimeSpan.FromSeconds(2), startedAtUtc: startedAtUtc);
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

var metadata = new BenchmarkRunMetadata(
    Framework: "Hangfire",
    DbConfig: "SqlServerStorage",
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
var outputDirectory = options.OutputDirectory ?? Path.Combine("Benchmarks", "Results", $"hangfire_sqlserver_{outputDirectorySuffix}");
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

Console.WriteLine("Dumping Hangfire DB diagnostics (job state distribution, queue/server counts)...");
var diagnosticsDirectory = Path.Combine(outputDirectory, "db-diagnostics");
Directory.CreateDirectory(diagnosticsDirectory);

var hangfireDbConnectionString = new SqlConnectionStringBuilder(((IDatabaseContainer)environment.DbContainer).GetConnectionString())
{
    InitialCatalog = HangfireTopologyBuilder.DatabaseName
}.ConnectionString;

await DumpQueryAsync(hangfireDbConnectionString, "SELECT COUNT(*) AS cnt FROM [HangFire].[Job]",
    Path.Combine(diagnosticsDirectory, "job-count.txt"));
await DumpQueryAsync(hangfireDbConnectionString, "SELECT StateName, COUNT(*) AS cnt FROM [HangFire].[Job] GROUP BY StateName",
    Path.Combine(diagnosticsDirectory, "jobs-by-state.txt"));
await DumpQueryAsync(hangfireDbConnectionString, "SELECT COUNT(*) AS cnt FROM [HangFire].[JobQueue]",
    Path.Combine(diagnosticsDirectory, "job-queue-count.txt"));
await DumpQueryAsync(hangfireDbConnectionString, "SELECT * FROM [HangFire].[Server]",
    Path.Combine(diagnosticsDirectory, "servers.txt"));

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

// Generic diagnostic dump -- doesn't assume exact column names/schema, just prints whatever
// columns exist so it survives schema differences without needing updates.
static async Task DumpQueryAsync(string connectionString, string sqlText, string outputPath)
{
    try
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sqlText, connection);
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
