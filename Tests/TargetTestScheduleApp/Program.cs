using System.Reflection;
using System.Text.Json;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Ioc.Extensions;
using StackExchange.Redis;
using TargetTestScheduleApp;
using TargetTestScheduleApp.Handlers;
using TargetTestScheduleApp.Redis;

// ConfigFromJson only sets RepoType as a string — it never calls a provider-specific method
// directly, so the CLR would never actually load these assemblies into the AppDomain, and their
// [JobMasterIocRegistration] providers (which register IMasterGenericRecordRepository etc.) would
// be invisible to the AppDomain-based reflection scan in
// JobMasterIocRegistrationAttribute.GetRegistrationTypes(). A `typeof(...)` reference alone is not
// enough to guarantee this (the JIT can elide an unused ldtoken), so load them explicitly by path.
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.Postgres.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.MySql.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.SqlServer.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.NatsJetStream.dll"));

var builder = WebApplication.CreateBuilder(args);

var clusterConfigsJson = Environment.GetEnvironmentVariable("JOBMASTER_CLUSTER_CONFIGS_JSON");
if (string.IsNullOrWhiteSpace(clusterConfigsJson))
{
    throw new InvalidOperationException("JOBMASTER_CLUSTER_CONFIGS_JSON environment variable must be set to a JSON array of cluster configs.");
}

var clusterConfigs = JsonSerializer.Deserialize<JsonElement[]>(clusterConfigsJson)
    ?? throw new InvalidOperationException("JOBMASTER_CLUSTER_CONFIGS_JSON did not deserialize to a JSON array.");

foreach (var clusterConfig in clusterConfigs)
{
    var json = clusterConfig.GetRawText();
    builder.Services.AddJobMasterCluster(c => c.ConfigFromJson(json));
}

var redisConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING")
    ?? throw new InvalidOperationException("REDIS_CONNECTION_STRING environment variable must be set.");

builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));
builder.Services.AddSingleton<IExecutionRecorder, RedisExecutionRecorder>();

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.MapPost("/schedule/{handlerType}", async (string handlerType, ScheduleRequest req, IJobMasterScheduler scheduler) =>
{
    if (req.QtyJobs < 1)
    {
        return Results.BadRequest("QtyJobs must be at least 1.");
    }

    var priority = (JobMasterPriority?)req.Priority;

    var tasks = new List<Task<JobContext>>();
    for (var i = 0; i < req.QtyJobs; i++)
    {
        var metadata = WritableMetadata.New().SetStringValue("TestIdentifier", req.TestIdentifier);
        tasks.Add(ScheduleHandler(handlerType, scheduler, metadata, req.ClusterId, req.AfterSeconds, priority));
    }

    var jobs = await Task.WhenAll(tasks);
    return Results.Ok(new ScheduleResponse(jobs.Select(j => j.Id).ToList()));
});

app.MapPost("/recurring-schedule/{handlerType}", async (string handlerType, RecurringScheduleRequest req, IJobMasterScheduler scheduler) =>
{
    var metadata = WritableMetadata.New().SetStringValue("TestIdentifier", req.TestIdentifier);
    var context = await RecurringScheduleHandler(handlerType, scheduler, req.ExpressionTypeId, req.Expression, metadata, req.ClusterId);
    return Results.Ok(new RecurringScheduleResponse(context.Id));
});

app.MapDelete("/recurring-schedule/{id}", async (Guid id, string? clusterId, IJobMasterScheduler scheduler) =>
{
    var cancelled = await scheduler.TryCancelRecurringAsync(id, clusterId);
    return cancelled ? Results.Ok() : Results.NotFound();
});

await app.Services.StartJobMasterRuntimeAsync();

app.Run();

return;

static Task<JobContext> ScheduleHandler(
    string handlerType,
    IJobMasterScheduler scheduler,
    IWritableMetadata metadata,
    string? clusterId,
    int? afterSeconds,
    JobMasterPriority? priority)
{
    return handlerType.ToLowerInvariant() switch
    {
        "fast" => afterSeconds.HasValue
            ? scheduler.OnceAfterAsync<TestAppFastHandler>(TimeSpan.FromSeconds(afterSeconds.Value), metadata: metadata, clusterId: clusterId, priority: priority)
            : scheduler.OnceNowAsync<TestAppFastHandler>(metadata: metadata, clusterId: clusterId, priority: priority),
        "normal" => afterSeconds.HasValue
            ? scheduler.OnceAfterAsync<TestAppNormalHandler>(TimeSpan.FromSeconds(afterSeconds.Value), metadata: metadata, clusterId: clusterId, priority: priority)
            : scheduler.OnceNowAsync<TestAppNormalHandler>(metadata: metadata, clusterId: clusterId, priority: priority),
        "slow" => afterSeconds.HasValue
            ? scheduler.OnceAfterAsync<TestAppSlowHandler>(TimeSpan.FromSeconds(afterSeconds.Value), metadata: metadata, clusterId: clusterId, priority: priority)
            : scheduler.OnceNowAsync<TestAppSlowHandler>(metadata: metadata, clusterId: clusterId, priority: priority),
        "verylong" => afterSeconds.HasValue
            ? scheduler.OnceAfterAsync<TestAppVeryLongHandler>(TimeSpan.FromSeconds(afterSeconds.Value), metadata: metadata, clusterId: clusterId, priority: priority)
            : scheduler.OnceNowAsync<TestAppVeryLongHandler>(metadata: metadata, clusterId: clusterId, priority: priority),
        _ => throw new ArgumentException($"Unknown handlerType '{handlerType}'. Expected one of: fast, normal, slow, verylong.")
    };
}

static Task<RecurringScheduleContext> RecurringScheduleHandler(
    string handlerType,
    IJobMasterScheduler scheduler,
    string expressionTypeId,
    string expression,
    IWritableMetadata metadata,
    string? clusterId)
{
    return handlerType.ToLowerInvariant() switch
    {
        "fast" => scheduler.RecurringAsync<TestAppFastHandler>(expressionTypeId, expression, metadata: metadata, clusterId: clusterId),
        "normal" => scheduler.RecurringAsync<TestAppNormalHandler>(expressionTypeId, expression, metadata: metadata, clusterId: clusterId),
        "slow" => scheduler.RecurringAsync<TestAppSlowHandler>(expressionTypeId, expression, metadata: metadata, clusterId: clusterId),
        "verylong" => scheduler.RecurringAsync<TestAppVeryLongHandler>(expressionTypeId, expression, metadata: metadata, clusterId: clusterId),
        _ => throw new ArgumentException($"Unknown handlerType '{handlerType}'. Expected one of: fast, normal, slow, verylong.")
    };
}
