using Hangfire;
using Hangfire.SqlServer;
using JobMaster.Benchmarks.Common.Recording;
using JobMaster.Benchmarks.Common.Scheduling;
using JobMaster.Benchmarks.HangfireHost.Jobs;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var sqlServerConnectionString = Environment.GetEnvironmentVariable("HANGFIRE_SQLSERVER_CONNECTION_STRING")
    ?? throw new InvalidOperationException("HANGFIRE_SQLSERVER_CONNECTION_STRING environment variable must be set.");

// No coordinator/leader concept, same as Quartz's clustered AdoJobStore -- every server instance is
// a fully symmetric peer dequeuing from the shared HangFire.JobQueue table via row-level locking.
// PrepareSchemaIfNecessary defaults to true (self-migrating schema, unlike Quartz's AdoJobStore --
// no vendored SQL script or schema-provisioner step needed here).
builder.Services.AddHangfire(config => config
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
    .UseSimpleAssemblyNameTypeSerializer()
    .UseRecommendedSerializerSettings()
    .UseSqlServerStorage(sqlServerConnectionString, new SqlServerStorageOptions
    {
        // Current documented recommended dequeue mode (lock-based, not fixed-interval polling) --
        // same rationale as using each other framework's real out-of-the-box current-best-practice
        // config rather than a legacy/undocumented default.
        QueuePollInterval = TimeSpan.Zero,
        SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
        DisableGlobalLocks = true,
        CommandBatchMaxTimeout = TimeSpan.FromMinutes(5)
    }));
builder.Services.AddHangfireServer(options =>
{
    options.ServerName = $"{Environment.MachineName}:{Guid.NewGuid()}";
});

var redisConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING")
    ?? throw new InvalidOperationException("REDIS_CONNECTION_STRING environment variable must be set.");

var runId = Environment.GetEnvironmentVariable("BENCHMARK_RUN_ID")
    ?? throw new InvalidOperationException("BENCHMARK_RUN_ID environment variable must be set.");

builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));
builder.Services.AddSingleton<ICompletionRecorder>(sp =>
    new RedisCompletionRecorder(sp.GetRequiredService<IConnectionMultiplexer>(), runId));

var app = builder.Build();

// Deliberately exercises the Redis connection (not just a bare 200) -- IConnectionMultiplexer is
// registered as a lazy singleton above, so without this a container can report healthy before it has
// ever actually connected to Redis. Under a burst of many workers starting at once, that let some
// workers' first real job execution race Redis's own startup and fail with a RedisConnectionException
// -- a benchmark-harness race, not a framework defect, but one that silently corrupted benchmark runs
// that hit it (originally found and documented for this host specifically).
app.MapGet("/health", (IServiceProvider sp) =>
{
    try
    {
        var redis = sp.GetRequiredService<IConnectionMultiplexer>();
        return redis.IsConnected ? Results.Ok(new { status = "ok" }) : Results.StatusCode(503);
    }
    catch
    {
        return Results.StatusCode(503);
    }
});

app.MapPost("/schedule-now", async (ScheduleNowRequest req, IBackgroundJobClient client) =>
{
    if (req.Count < 1)
    {
        return Results.BadRequest("Count must be at least 1.");
    }

    // Enqueue has no async overload, so real concurrency means offloading to the thread pool.
    var jobIds = new Guid[req.Count];
    var tasks = new Task[req.Count];
    for (var i = 0; i < req.Count; i++)
    {
        var index = i;
        var jobId = Guid.NewGuid();
        jobIds[index] = jobId;
        tasks[index] = Task.Run(() => client.Enqueue<NoOpHangfireJob>(h => h.Execute(jobId)));
    }

    await Task.WhenAll(tasks);
    return Results.Ok(new ScheduleResponse(jobIds.ToList()));
});

app.MapPost("/schedule-after", async (ScheduleAfterRequest req, IBackgroundJobClient client) =>
{
    if (req.Count < 1)
    {
        return Results.BadRequest("Count must be at least 1.");
    }

    var delay = TimeSpan.FromMinutes(req.DelayMinutes);
    var jobIds = new Guid[req.Count];
    var tasks = new Task[req.Count];
    for (var i = 0; i < req.Count; i++)
    {
        var index = i;
        var jobId = Guid.NewGuid();
        jobIds[index] = jobId;
        tasks[index] = Task.Run(() => client.Schedule<NoOpHangfireJob>(h => h.Execute(jobId), delay));
    }

    await Task.WhenAll(tasks);
    return Results.Ok(new ScheduleResponse(jobIds.ToList()));
});

app.Run();
