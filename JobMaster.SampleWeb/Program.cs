using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Api.AspNetCore;
using JobMaster.Ioc.Extensions;
using JobMaster.MySql;
using JobMaster.NatsJetStream;
using JobMaster.SampleWeb;
using JobMaster.Postgres;
using JobMaster.Postgres.Agents;
using JobMaster.SqlBase;
using JobMaster.SqlServer;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Serilog;
using System.Text.RegularExpressions;

var builder = WebApplication.CreateBuilder(args);

if (builder.Environment.IsDevelopment())
{
    builder.Configuration.AddUserSecrets<Program>(optional: true);
}

static string ApplySecrets(string value, IConfiguration config)
{
    if (string.IsNullOrWhiteSpace(value))
    {
        return value;
    }

    var replaced = value;

    var secretTokenMatches = Regex.Matches(value, @"\bsecret_[A-Za-z0-9_]+\b");
    foreach (Match match in secretTokenMatches)
    {
        var token = match.Value;
        if (string.IsNullOrWhiteSpace(token))
        {
            continue;
        }

        var secretValue = config[token];
        if (string.IsNullOrWhiteSpace(secretValue))
        {
            continue;
        }

        replaced = replaced.Replace(token, secretValue, StringComparison.Ordinal);
    }

    var bracketTokenMatches = Regex.Matches(value, @"\[[A-Za-z0-9_]+\]");
    foreach (Match match in bracketTokenMatches)
    {
        var token = match.Value;
        if (string.IsNullOrWhiteSpace(token) || token.Length < 3)
        {
            continue;
        }

        var key = token.Substring(1, token.Length - 2);
        if (string.IsNullOrWhiteSpace(key))
        {
            continue;
        }

        var secretValue = config[key];
        if (string.IsNullOrWhiteSpace(secretValue))
        {
            continue;
        }

        replaced = replaced.Replace(token, secretValue, StringComparison.Ordinal);
    }

    return replaced;
}

var masterPostgres = ApplySecrets(
    builder.Configuration["JobMaster:SampleWeb:MasterPostgres"]
    ?? "Host=[POSTGRES_HOST];Port=[POSTGRES_PORT];Database=jobmaster;Username=[POSTGRES_USER];Password=[POSTGRES_PASSWORD];Maximum Pool Size=300",
    builder.Configuration);

var natsUrl = ApplySecrets(
    builder.Configuration["JobMaster:SampleWeb:NatsJetStream"]
    ?? "nats://[NATS_USER]:[NATS_PASSWORD]@[NATS_HOST]:[NATS_PORT]",
    builder.Configuration);

var standalonePostgres = ApplySecrets(
    builder.Configuration["JobMaster:SampleWeb:StandalonePostgres"]
    ?? "Host=[POSTGRES_HOST];Port=[POSTGRES_PORT];Database=jobmaster_standalone;Username=[POSTGRES_USER];Password=[POSTGRES_PASSWORD];Maximum Pool Size=300",
    builder.Configuration);

var apiKeyOwner = ApplySecrets(
    builder.Configuration["JobMaster:SampleWeb:ApiKeyOwner"]
    ?? "[JM_API_KEY_OWNER]",
    builder.Configuration);

var apiKey = ApplySecrets(
    builder.Configuration["JobMaster:SampleWeb:ApiKey"]
    ?? "[JM_API_KEY]",
    builder.Configuration);

var apiUser = ApplySecrets(
    builder.Configuration["JobMaster:SampleWeb:ApiUser"]
    ?? "[JM_API_USER]",
    builder.Configuration);

var apiPassword = ApplySecrets(
    builder.Configuration["JobMaster:SampleWeb:ApiPassword"]
    ?? "[JM_API_PASSWORD]",
    builder.Configuration);

builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("Cluster-1")
          .ClusterTransientThreshold(TimeSpan.FromMinutes(1))
          .DebugJsonlFileLogger("/home/hugo/logs/Cluster-1.log")
          .ClusterMode(ClusterMode.Active);

    // Master database (must be SQL)
    config.UsePostgresForMaster(masterPostgres);
    
    // config.AddAgentConnectionConfig("Pg-1")
    //       .UsePostgresForAgent("Host=[POSTGRES_HOST];Port=[POSTGRES_PORT];Database=agent_pg1;Username=[POSTGRES_USER];Password=[POSTGRES_PASSWORD]");
    //
    // config.AddAgentConnectionConfig("My-1")
    //       .UseMySqlForAgent("Server=[MYSQL_HOST];Port=[MYSQL_PORT];Database=agent_my1;User ID=[MYSQL_USER];Password=[MYSQL_PASSWORD];");
    //
    // config.AddAgentConnectionConfig("Sql-1")
    //     .UseSqlServerForAgent("Server=[SQL_SERVER_HOST];Initial Catalog=agent_sql1;User Id=[SQL_SERVER_USER];Password=[SQL_SERVER_PASSWORD];Encrypt=False;TrustServerCertificate=True;");

    
    config.AddAgentConnectionConfig("Nats-1")
          .UseNatsJetStream(natsUrl);

    var isConsumer = Environment.GetEnvironmentVariable("CONSUMER")?.ToUpperInvariant() == "TRUE";
    if (isConsumer)
    {
       config.AddWorker()
           .AgentConnName("Nats-1")
           .BucketQtyConfig(JobMasterPriority.Medium, 1)
           .WorkerBatchSize(1000)
           .SetWorkerMode(AgentWorkerMode.Full);

       config.AddWorker()
           .AgentConnName("Nats-1")
           .BucketQtyConfig(JobMasterPriority.Medium, 1)
           .WorkerBatchSize(1000)
           .SetWorkerMode(AgentWorkerMode.Drain);
    }
});
builder.Services.AddJobMasterCluster(c => {
        
    c.UseStandaloneCluster().ClusterId("Cluster-Standalone-1")
        .UsePostgres(standalonePostgres)
        .SetAsDefault()
        .AddWorker();
    
});

builder.Services.UseJobMasterApi(o =>
{
    o.BasePath = "/jm-api";
    o.RequireAuthentication = true;
    o.EnableSwagger = true;
    
    o.UseApiKeyAuth().AddApiKey(apiKeyOwner, apiKey);
    o.UseUserPwdAuth().AddUserPwd(apiUser, apiPassword);
});



builder.Services.AddEndpointsApiExplorer();

builder.Services.AddSwaggerGen();
Log.Logger = new LoggerConfiguration()
    .WriteTo.Seq("http://localhost:5341/")
    .MinimumLevel.Debug()
    .CreateLogger();

Log.Information("Starting up");

builder.Services.AddSerilog();

var app = builder.Build();

app.MapJobMasterApi();

await app.Services.StartJobMasterRuntimeAsync();


app.UseSwagger();
app.UseSwaggerUI();
app.UseHttpsRedirection();


app.MapPost("/schedule-job", async(int qty, string ? lane, string? clusterId, TimeSpan? delay, IJobMasterScheduler jobScheduler) =>
{
    if (string.IsNullOrWhiteSpace(lane)) lane = null;

    var meta = WritableMetadata.New().SetStringValue("MyMetadata", "MyValue");
    var tasks = new List<Task>();
    for (int i = 0; i < qty; i++)
    {
        var data = WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName());
        if (delay.HasValue)
            tasks.Add(jobScheduler.OnceAfterAsync<HelloJobHandler>(delay.Value, data, metadata: meta, workerLane: lane, clusterId: clusterId));
        else
            tasks.Add(jobScheduler.OnceNowAsync<HelloJobHandler>(data, metadata: meta, workerLane: lane, clusterId: clusterId));
    }

    await Task.WhenAll(tasks);
    
    return "Qty of scheduled jobs: " + qty;
    
}).WithOpenApi();


app.MapPost("/recurring-schedule-job", (string expressionType, string expression, string? lane, IJobMasterScheduler jobScheduler) =>
{
    jobScheduler.Recurring<HelloJobHandler>(expressionType, expression, WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName()), metadata: WritableMetadata.New().SetStringValue("expression", expression), workerLane: lane);
    
}).WithOpenApi();

app.MapDelete("/cancel-recurring-schedule-job", (Guid id, IJobMasterScheduler jobScheduler) =>
{
    jobScheduler.CancelRecurring(id);
}).WithOpenApi();
//
// app.MapPost("/stop-immediately", async (IJob) =>
// {
//     if (JobMasterRuntime.Instance != null)
//     {
//         await JobMasterRuntime.Instance.StopImmediatelyAsync();
//     }
//     
//     return "Stop initiated";
// }).WithOpenApi();



app.Run();



// namespace JobMaster.SampleWeb
// {
//     public class HelloJobHandler : IJobHandler
//     {
//         public HelloJobHandler()
//         {
//         }
//     
//         public Task HandleAsync(Job job)
//         {
//             var name = job.Data.GetStringValue("Name") ?? string.Empty;
//             SayHello(name);
//
//             RedisJobs.AddJobExecutedId(job.Id);
//             RedisJobs.RemoveScheduledJobId(job.Id);
//         
//             return Task.CompletedTask;
//         }
//
//         public static void SayHello(string name)
//         {
//             Console.WriteLine($"Hello {name}");
//         }
//     }
//
//
//
//     public class HelloJobLongRunHandler : IJobHandler
//     {
//         public async Task HandleAsync(Job job)
//         {
//             for (var i = 0; i < 100; i++)
//             {
//                 await Task.Delay(i * 100);
//             }
//         
//             var name = job.Data.GetStringValue("Name");
//             Console.WriteLine($"Hello {name}");
//         }
//     }
// }
