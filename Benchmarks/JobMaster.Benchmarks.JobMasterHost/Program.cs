using System.Reflection;
using System.Text.Json;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Benchmarks.Common.Recording;
using JobMaster.Benchmarks.Common.Scheduling;
using JobMaster.Benchmarks.JobMasterHost.Handlers;
using JobMaster.Ioc.Extensions;
using StackExchange.Redis;

// ConfigFromJson only sets RepoType as a string -- it never calls a provider-specific method
// directly, so the CLR would never actually load these assemblies into the AppDomain, and their
// [JobMasterIocRegistration] providers would be invisible to the AppDomain-based reflection scan.
// Same trick TargetTestScheduleApp uses.
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.Postgres.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.MySql.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.SqlServer.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.NatsJetStream.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.RavenDb.dll"));

var builder = WebApplication.CreateBuilder(args);

// JobMaster's own runners are silent at the default Information level -- turned up to Debug here
// so a benchmark run's container logs actually show bucket-assignment/execution activity, not just
// ASP.NET's own request-handling lines.
builder.Logging.AddFilter("JobMaster", LogLevel.Debug);

var clusterConfigsJson = Environment.GetEnvironmentVariable("JOBMASTER_CLUSTER_CONFIGS_JSON")
    ?? throw new InvalidOperationException("JOBMASTER_CLUSTER_CONFIGS_JSON environment variable must be set to a JSON array of cluster configs.");

var clusterConfigs = JsonSerializer.Deserialize<JsonElement[]>(clusterConfigsJson)
    ?? throw new InvalidOperationException("JOBMASTER_CLUSTER_CONFIGS_JSON did not deserialize to a JSON array.");

foreach (var clusterConfig in clusterConfigs)
{
    var json = clusterConfig.GetRawText();
    builder.Services.AddJobMasterCluster(c => c.ConfigFromJson(json));
}

var redisConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING")
    ?? throw new InvalidOperationException("REDIS_CONNECTION_STRING environment variable must be set.");

var runId = Environment.GetEnvironmentVariable("BENCHMARK_RUN_ID")
    ?? throw new InvalidOperationException("BENCHMARK_RUN_ID environment variable must be set.");

builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));
builder.Services.AddSingleton<ICompletionRecorder>(sp =>
    new RedisCompletionRecorder(sp.GetRequiredService<IConnectionMultiplexer>(), runId));

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.MapPost("/schedule-now", async (ScheduleNowRequest req, IJobMasterScheduler scheduler) =>
{
    if (req.Count < 1)
    {
        return Results.BadRequest("Count must be at least 1.");
    }

    var tasks = new List<Task<JobContext>>(req.Count);
    for (var i = 0; i < req.Count; i++)
    {
        tasks.Add(scheduler.OnceNowAsync<NoOpBenchmarkHandler>());
    }

    var jobs = await Task.WhenAll(tasks);
    return Results.Ok(new ScheduleResponse(jobs.Select(j => j.Id).ToList()));
});

app.MapPost("/schedule-after", async (ScheduleAfterRequest req, IJobMasterScheduler scheduler) =>
{
    if (req.Count < 1)
    {
        return Results.BadRequest("Count must be at least 1.");
    }

    var delay = TimeSpan.FromMinutes(req.DelayMinutes);
    var tasks = new List<Task<JobContext>>(req.Count);
    for (var i = 0; i < req.Count; i++)
    {
        tasks.Add(scheduler.OnceAfterAsync<NoOpBenchmarkHandler>(delay));
    }

    var jobs = await Task.WhenAll(tasks);
    return Results.Ok(new ScheduleResponse(jobs.Select(j => j.Id).ToList()));
});

await app.Services.StartJobMasterRuntimeAsync();

app.Run();
