using JobMaster.Contracts;
using JobMaster.Contracts.Models;
using JobMaster.Ioc.Extensions;
using JobMaster.MySql;
using JobMaster.NatJetStream;
using JobMaster.SampleWeb;
using JobMaster.Postgres;
using JobMaster.Postgres.Agents;
using JobMaster.SqlServer;
using Microsoft.AspNetCore.Mvc;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("Cluster-1")
          .ClusterTransientThreshold(TimeSpan.FromMinutes(1))
          .DebugJsonlFileLogger("/home/hugo/logs/Cluster-1.log")
          .ClusterMode(ClusterMode.Active);

    // Master database (must be SQL)
    config.UsePostgresForMaster("Host=localhost;Port=5432;Database=jobmaster;Username=postgres;Password=postgres;Maximum Pool Size=300");

    // Agent connection pools using different providers
    config.AddAgentConnectionConfig("Pg-1")
          .UsePostgresForAgent("Host=localhost;Port=5432;Database=agent_pg1;Username=postgres;Password=postgres");

    config.AddAgentConnectionConfig("My-1")
          .UseMySqlForAgent("Server=localhost;Port=3306;Database=agent_my1;User ID=root;Password=root;");

    config.AddAgentConnectionConfig("Sql-1")
        .UseSqlServerForAgent("Server=localhost,1433;Initial Catalog=agent_sql1;User Id=sa;Password=Passw0rd!;Encrypt=False;TrustServerCertificate=True;");

    
    config.AddAgentConnectionConfig("Nats-1")
          .UseNatJetStream("nats://jmuser:jmpass@localhost:4222");

    var isConsumer = Environment.GetEnvironmentVariable("CONSUMER")?.ToUpperInvariant() == "TRUE";
    if (isConsumer)
    {
        // Worker bound to Postgres agent
       //  config.AddWorker()
       //        .WorkerName("worker-pg")
       //        .AgentConnName("Pg-1")
       //        .BucketQtyConfig(JobMasterPriority.Medium, 1)
       //        .SetWorkerMode(AgentWorkerMode.Standalone);
       //
       // // Worker bound to MySQL agent
       //  config.AddWorker()
       //        .WorkerName("worker-mysql")
       //        .AgentConnName("My-1")
       //        .BucketQtyConfig(JobMasterPriority.Medium, 1)
       //        .SetWorkerMode(AgentWorkerMode.Standalone);
       //  
       //  // Worker bound to SQL Server agent
       //  config.AddWorker()
       //        .WorkerName("worker-sqlserver")
       //        .AgentConnName("Sql-1")
       //        .BucketQtyConfig(JobMasterPriority.Medium, 1)
       //        .SetWorkerMode(AgentWorkerMode.Standalone);
        
        config.AddWorker()
              .WorkerName("worker-nats")
              .AgentConnName("Nats-1")
              .BucketQtyConfig(JobMasterPriority.Medium, 1)
              .SetWorkerMode(AgentWorkerMode.Standalone);
    }
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

await app.Services.StartJobMasterRuntimeAsync();

app.UseSwagger();
app.UseSwaggerUI();
app.UseHttpsRedirection();

app.MapPost("/schedule-job", async (int qty, string? lane, TimeSpan? delay, IJobMasterScheduler jobScheduler) =>
{
    if (string.IsNullOrWhiteSpace(lane)) lane = null;

    var meta = WritableMetadata.New().SetStringValue("MyMetadata", "MyValue");
    var degree = Math.Max(1, Environment.ProcessorCount * 4);
    using var sem = new SemaphoreSlim(degree);

    var tasks = Enumerable.Range(0, qty).Select(async _ =>
    {
        await sem.WaitAsync();
        try
        {
            var data = WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName());
            if (delay.HasValue)
                await jobScheduler.OnceAfterAsync<HelloJobHandler>(delay.Value, data, metadata: meta, workerLane: lane);
            else
                await jobScheduler.OnceNowAsync<HelloJobHandler>(data, metadata: meta, workerLane: lane);
        }
        finally
        {
            sem.Release();
        }
    });

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
