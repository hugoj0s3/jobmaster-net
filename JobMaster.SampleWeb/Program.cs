using Hangfire;
using JobMaster.Contracts;
using JobMaster.Contracts.Models;
using JobMaster.Ioc.Extensions;
using JobMaster.SampleWeb;
using JobMaster.Postgres;
using JobMaster.Postgres.Agents;
using JobMaster.UI;
using Microsoft.AspNetCore.Mvc;
using Serilog;

var builder = WebApplication.CreateBuilder(args);
// builder.UseJobMasterServer(config =>
// {
//     ConfigBuilderExtensions.UseSqlServer((AgentConfigBuilder)config.AgentConfig
//         .AgentConnectionId("Test"), "Server=localhost;Initial Catalog=JobAgent;user=sa;pwd=Password#123;TrustServerCertificate=true");
//     
//     ConfigBuilderExtensions.UseSqlServer((MasterConfigBuilder)config.MasterConfig, "Server=localhost;Initial Catalog=JobMaster;user=sa;pwd=Password#123;TrustServerCertificate=true");
//     var qtyBucketsStr = Environment.GetEnvironmentVariable("QTY_BUCKETS");
//     var qtyBuckets = qtyBucketsStr != null ? int.Parse(qtyBucketsStr) : 0;
//     config.BucketNumbersConfig.SetQtyBucketsPerPriority(0, qtyBuckets, 0);
// });

builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("Cluster-1")
        .ClusterTransientThreshold(TimeSpan.FromMinutes(1))
        .ClusterMode(ClusterMode.Active);
    
    config.UsePostgresForMaster("Host=localhost;Port=5432;Database=master;Username=postgres;Password=postgres;Maximum Pool Size=300");

    config.AddAgentConnectionConfig("Postgres-1")
        .UsePostgresForAgent("Host=localhost;Port=5432;Database=agent_1;Username=postgres;Password=postgres;");
    
    config.AddAgentConnectionConfig("Postgres-2")
        .UsePostgresForAgent("Host=localhost;Port=5432;Database=agent_2;Username=postgres;Password=postgres;");
    
    config.AddAgentConnectionConfig("Postgres-3")
        .UsePostgresForAgent("Host=localhost;Port=5432;Database=agent_3;Username=postgres;Password=postgres;");
    
    var isConsumer = Environment.GetEnvironmentVariable("CONSUMER")?.ToUpperInvariant() == "TRUE";
    
    if (isConsumer)
    {
        config.AddWorker()
            .AgentConnName("Postgres-1")
            .BucketQtyConfig(JobMasterPriority.Medium, 1)
            .SetWorkerMode(AgentWorkerMode.Standalone);
    
        config.AddWorker()
            .WorkerName("Postgres-2")
            .AgentConnName("Postgres-2")
            .WorkerLane("Lane1")
            .BucketQtyConfig(JobMasterPriority.Medium, 1)
            .SetWorkerMode(AgentWorkerMode.Standalone);
    
        config.AddWorker()
            .AgentConnName("Postgres-3")
            .WorkerLane("Lane2")
            .BucketQtyConfig(JobMasterPriority.Medium, 1)
            .SetWorkerMode(AgentWorkerMode.Standalone);
    }
   

    // config.AddWorker("Postgres-2")
    //     .SetWorkerBucketQtyConfig(JobMasterPriority.VeryLow, 1)
    //     .SetWorkerBucketQtyConfig(JobMasterPriority.Low, 2)
    //     .SetWorkerBucketQtyConfig(JobMasterPriority.Medium, 3)
    //     .SetWorkerBucketQtyConfig(JobMasterPriority.High, 4)
    //     .SetWorkerBucketQtyConfig(JobMasterPriority.Critical, 5);
});

//
// builder.Services.AddHangfire(configuration => configuration
//     .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
//     .UseSimpleAssemblyNameTypeSerializer()
//     .UseRecommendedSerializerSettings()
//     .UseSqlServerStorage("Server=localhost;Initial Catalog=Hangfire;user=sa;pwd=Password#123;TrustServerCertificate=true"));
//
// builder.Services.AddHangfireServer();


builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

Log.Logger = new LoggerConfiguration()
    .WriteTo.Seq("http://localhost:5341/")
    .MinimumLevel.Debug()
    .CreateLogger();

Log.Information("Starting up");

builder.Services.AddSerilog();

var app = builder.Build();

await app.Services.StartJobMasterRuntimeAsync();

app.UseSwagger();
app.UseSwaggerUI();
app.UseHttpsRedirection();

app.MapPost("/schedule-job", async (int qty, TimeSpan? delay, IJobMasterScheduler jobScheduler) =>
{
    var tasks = new List<Task>();
    for (var i = 0; i < qty; i++)
    {
        var writableMetadata = WritableMetadata.New().SetStringValue("MyMetadata", "MyMetadataValue");
        if (!delay.HasValue)
        {
            tasks.Add(jobScheduler.OnceNowAsync<HelloJobHandler>(WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName()), metadata: writableMetadata));
        }
        else
        {
            tasks.Add(jobScheduler.OnceAfterAsync<HelloJobHandler>(delay.Value, WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName()), metadata: writableMetadata));
        }
    }
    
    
    await Task.WhenAll(tasks);
    
    return "Qty of scheduled jobs: " + qty;
    
}).WithOpenApi();

app.MapPost("/schedule-job", async (IJobMasterScheduler jobScheduler) =>
{
    var msg = WriteableMessageData.New().SetStringValue("Name", "John Doe");
    await jobScheduler.OnceNowAsync<HelloJobHandler>(msg);
    
}).WithOpenApi();

app.MapPost("/schedule-job2", (int qty, TimeSpan? delay, IJobMasterScheduler jobScheduler) =>
{
    for (var i = 0; i < qty; i++)
    {
        // if (i % 10 == 0)
        // {
        //     Thread.Sleep(50);
        // }
        
        if (!delay.HasValue)
        {
            jobScheduler.OnceNow<HelloJobHandler2>(WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName()), metadata: WritableMetadata.New().SetStringValue("MyMetadata", "MyMetadataValue"));
        }
        else
        {
            jobScheduler.OnceAfter<HelloJobHandler2>(delay.Value, WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName()), metadata: WritableMetadata.New().SetStringValue("MyMetadata", "MyMetadataValue"));
        }
    }
    
    return "Qty of scheduled jobs: " + qty;
    
}).WithOpenApi();


app.MapPost("/recurring-schedule-job", (string expressionType, string expression, IJobMasterScheduler jobScheduler) =>
{
    jobScheduler.Recurring<HelloJobHandler>(expressionType, expression, WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName()), metadata: WritableMetadata.New().SetStringValue("expression", expression));
    
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
