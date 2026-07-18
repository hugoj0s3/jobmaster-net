using System.Reflection;
using System.Text.Json;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Ioc.Extensions;
using StackExchange.Redis;
using TargetTestRecurringApp;
using TargetTestRecurringApp.Handlers;
using TargetTestRecurringApp.Redis;

// See TargetTestScheduleApp/Program.cs for why this is necessary: ConfigFromJson only sets
// RepoType as a string, so the CLR never naturally loads the provider assembly, and the
// [JobMasterIocRegistration] AppDomain-reflection scan can't see types in an unloaded assembly.
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

app.MapPost("/recurring-schedule/{handlerType}", async (string handlerType, RecurringScheduleRequest req, IJobMasterScheduler scheduler) =>
{
    if (!string.Equals(handlerType, "tick", StringComparison.OrdinalIgnoreCase))
    {
        return Results.BadRequest($"Unknown handlerType '{handlerType}'. Expected 'tick'.");
    }

    var metadata = WritableMetadata.New().SetStringValue("TestIdentifier", req.TestIdentifier);
    var context = await scheduler.RecurringAsync<RecurringTickHandler>(
        req.ExpressionTypeId, req.Expression, metadata: metadata, clusterId: req.ClusterId);

    return Results.Ok(new RecurringScheduleResponse(context.Id));
});

app.MapDelete("/recurring-schedule/{id}", async (Guid id, string? clusterId, IJobMasterScheduler scheduler) =>
{
    var cancelled = await scheduler.TryCancelRecurringAsync(id, clusterId);
    return cancelled ? Results.Ok() : Results.NotFound();
});

await app.Services.StartJobMasterRuntimeAsync();

app.Run();
