using JobMaster.Benchmarks.Common.Containers;
using JobMaster.Benchmarks.Common.Recording;
using JobMaster.Benchmarks.Common.Scheduling;
using JobMaster.Benchmarks.QuartzHost.Jobs;
using Quartz;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var dbConnectionString = Environment.GetEnvironmentVariable("QUARTZ_DB_CONNECTION_STRING")
    ?? throw new InvalidOperationException("QUARTZ_DB_CONNECTION_STRING environment variable must be set.");
var dbEngine = Enum.Parse<DbEngine>(Environment.GetEnvironmentVariable("QUARTZ_DB_ENGINE")
    ?? throw new InvalidOperationException("QUARTZ_DB_ENGINE environment variable must be set."));

// AUTO generates a unique instance ID per process -- clustering coordination is purely row-locking
// against the shared QRTZ_LOCKS/QRTZ_FIRED_TRIGGERS tables, no coordinator/leader concept, unlike
// JobMaster's bucket-assignment model.
builder.Services.AddQuartz(q =>
{
    q.SchedulerId = "AUTO";

    // Defaults (MaxBatchSize=1, thread pool MaxConcurrency=10) throttle acquisition far below what
    // a 0.5-CPU container can actually do -- confirmed via a real benchmark run where worker CPU sat
    // at ~12% avg/36% max under a 2500/min sustained load while a large backlog of WAITING triggers
    // built up. 50 matches the batch-size convention already used elsewhere in this benchmark
    // (LoadGenerator.TargetJobsPerTick, JobMaster's dispatch MaxBatchSizeForBulkOperation cap) rather
    // than an arbitrary new number. The no-op job body is near-instant (one Redis write), so thread
    // pool size mostly bounds concurrent DB completion-writes, not CPU-bound execution time -- 25 is
    // a modest bump from default, not a large guess, to avoid just shifting the bottleneck blindly.
    q.MaxBatchSize = 50;
    q.UseDefaultThreadPool(tp => tp.MaxConcurrency = 25);

    q.UsePersistentStore(s =>
    {
        s.PerformSchemaValidation = false; // schema is provisioned up front by QuartzSchemaProvisioner
        s.UseProperties = true;

        switch (dbEngine)
        {
            case DbEngine.Postgres:
                s.UsePostgres(sql =>
                {
                    sql.ConnectionString = dbConnectionString;
                    sql.TablePrefix = "QRTZ_";
                });
                break;
            case DbEngine.MySql:
                s.UseMySqlConnector(sql =>
                {
                    sql.ConnectionString = dbConnectionString;
                    sql.TablePrefix = "QRTZ_";
                });
                break;
            default:
                s.UseSqlServer(sql =>
                {
                    sql.ConnectionString = dbConnectionString;
                    sql.TablePrefix = "QRTZ_";
                });
                break;
        }

        s.UseSystemTextJsonSerializer(_ => { });
        s.UseClustering(c =>
        {
            c.CheckinInterval = TimeSpan.FromSeconds(10);
            c.CheckinMisfireThreshold = TimeSpan.FromSeconds(20);
        });
    });
});
builder.Services.AddQuartzHostedService(o => o.WaitForJobsToComplete = false);

var redisConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING")
    ?? throw new InvalidOperationException("REDIS_CONNECTION_STRING environment variable must be set.");

var runId = Environment.GetEnvironmentVariable("BENCHMARK_RUN_ID")
    ?? throw new InvalidOperationException("BENCHMARK_RUN_ID environment variable must be set.");

builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));
builder.Services.AddSingleton<ICompletionRecorder>(sp =>
    new RedisCompletionRecorder(sp.GetRequiredService<IConnectionMultiplexer>(), runId));

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.MapPost("/schedule-now", async (ScheduleNowRequest req, ISchedulerFactory schedulerFactory, CancellationToken ct) =>
{
    if (req.Count < 1)
    {
        return Results.BadRequest("Count must be at least 1.");
    }

    var scheduler = await schedulerFactory.GetScheduler(ct);
    var jobIds = new Guid[req.Count];
    var tasks = new Task[req.Count];

    for (var i = 0; i < req.Count; i++)
    {
        var index = i;
        var jobId = Guid.NewGuid();
        jobIds[index] = jobId;

        var job = JobBuilder.Create<NoOpQuartzJob>()
            .WithIdentity(jobId.ToString(), "benchmark")
            .UsingJobData(NoOpQuartzJob.JobIdDataKey, jobId.ToString())
            .Build();

        var trigger = TriggerBuilder.Create()
            .WithIdentity(jobId.ToString(), "benchmark")
            .StartNow()
            .Build();

        tasks[index] = scheduler.ScheduleJob(job, trigger, ct);
    }

    await Task.WhenAll(tasks);
    return Results.Ok(new ScheduleResponse(jobIds.ToList()));
});

app.MapPost("/schedule-after", async (ScheduleAfterRequest req, ISchedulerFactory schedulerFactory, CancellationToken ct) =>
{
    if (req.Count < 1)
    {
        return Results.BadRequest("Count must be at least 1.");
    }

    var scheduler = await schedulerFactory.GetScheduler(ct);
    var delay = TimeSpan.FromMinutes(req.DelayMinutes);
    var jobIds = new Guid[req.Count];
    var tasks = new Task[req.Count];

    for (var i = 0; i < req.Count; i++)
    {
        var index = i;
        var jobId = Guid.NewGuid();
        jobIds[index] = jobId;

        var job = JobBuilder.Create<NoOpQuartzJob>()
            .WithIdentity(jobId.ToString(), "benchmark")
            .UsingJobData(NoOpQuartzJob.JobIdDataKey, jobId.ToString())
            .Build();

        var trigger = TriggerBuilder.Create()
            .WithIdentity(jobId.ToString(), "benchmark")
            .StartAt(DateTimeOffset.UtcNow.Add(delay))
            .Build();

        tasks[index] = scheduler.ScheduleJob(job, trigger, ct);
    }

    await Task.WhenAll(tasks);
    return Results.Ok(new ScheduleResponse(jobIds.ToList()));
});

app.Run();
