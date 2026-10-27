using System.Reflection;
using System.Text.Json;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Ioc.Extensions;
using StackExchange.Redis;
using TargetTestClusterIdApp;
using TargetTestClusterIdApp.Handlers;
using TargetTestClusterIdApp.Redis;

// See TargetTestScheduleApp/Program.cs for why this is necessary: ConfigFromJson only sets
// RepoType as a string, so the CLR never naturally loads the provider assembly, and the
// [JobMasterIocRegistration] AppDomain-reflection scan can't see types in an unloaded assembly.
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.Postgres.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.MySql.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.SqlServer.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.NatsJetStream.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.RavenDb.dll"));

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
builder.Services.AddSingleton<ExecutionRecorder>();

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

// Same request/response contract as TargetTestScheduleApp's /schedule/{handlerType}, so the scenario side can
// reuse the existing ScheduleClient. ClusterId is passed straight through: null (the default) is the case
// under test here -- the cluster must then come from the handler/definition, not the call.
app.MapPost("/schedule/{handlerType}", async (string handlerType, ScheduleRequest req, IJobMasterScheduler scheduler) =>
{
    if (req.QtyJobs < 1)
    {
        return Results.BadRequest("QtyJobs must be at least 1.");
    }

    var jobs = new List<JobScheduleReceipt>();
    for (var i = 0; i < req.QtyJobs; i++)
    {
        var metadata = WritableMetadata.New().SetStringValue("TestIdentifier", req.TestIdentifier);
        jobs.Add(await (handlerType.ToLowerInvariant() switch
        {
            "attribute" => scheduler.OnceNowAsync<AttributeOnceHandler>(metadata: metadata, clusterId: req.ClusterId),
            "advanced" => scheduler.Advanced.OnceNowAsync<AdvancedOnceDefinitionAttribute>(metadata: metadata, clusterId: req.ClusterId),
            "default" => scheduler.OnceNowAsync<DefaultOnceHandler>(metadata: metadata, clusterId: req.ClusterId),
            _ => throw new ArgumentException($"Unknown handlerType '{handlerType}'. Expected one of: attribute, advanced, default.")
        }));
    }

    return Results.Ok(new ScheduleResponse(jobs.Select(j => j.GetJobContext().Id).ToList()));
});

await app.Services.StartJobMasterRuntimeAsync();

app.Run();
