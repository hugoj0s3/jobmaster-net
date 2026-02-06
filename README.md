# JobMaster .Net
## Distributed job orchestration engine for .NET. Oriented to horizontal scalling and flexibility.

JobMaster is a framework designed to manage and execute background tasks across a distributed cluster. By decoupling coordination from execution, it allows developers to scale their infrastructure horizontally based on workload demands.

[![NuGet (pre)](https://img.shields.io/nuget/vpre/JobMaster?label=JobMaster)](https://www.nuget.org/packages/JobMaster)

## 📋 Overview
JobMaster provides a architecture to handle job lifecycles. It is built to be transport-agnostic, supporting RDBMS (PostgreSQL, SQL Server, MySQL) and Message Brokers (NATS JetStream).

## 🚀 Quick Start (Standalone Setup)
Standalone setup is the easiest way to start. It uses a single database connection for both coordination and job storage, with no external brokers/database required.

### Configuration
Register JobMaster in your `Program.cs`. This sets up the database and attaches a background worker automatically.

```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.UseStandaloneCluster()
          .ClusterId("Local-Cluster-01")
          .UsePostgres("Host=localhost;Database=jobmaster_db;Username=postgres;Password=pwd")
          .AddWorker();   // Starts the worker to execute jobs.
});

var app = builder.Build();

// Start the JobMaster runtime loops
await app.Services.StartJobMasterRuntimeAsync();
```

### 🛠️ Implementing a Job Handler

A Job Handler is a simple class that contains your background logic. JobMaster handles the instantiation and execution; you just focus on the code.

### Basic Implementation
Create a class that implements the `IJobHandler` interface.

```csharp
using JobMaster.Sdk.Abstractions.Models;

public class ProcessImageHandler : IJobHandler
{
    // The HandleAsync method is the entry point for the worker
    public async Task HandleAsync(JobContext job)
    {
        // 1. Retrieve data sent during scheduling
        var imageUrl = job.MsgData.GetStringValue("SourceUrl");
        var filterType = job.MsgData.GetStringValue("Filter");

        Console.WriteLine($"[Job {job.Id}] Processing image: {imageUrl} with {filterType}");

        // 2. Perform your business logic
        await Task.Delay(500); // Simulating work

        // 3. Handlers are async-ready
        await Task.CompletedTask;
    }
}
```

### Schedule from a Minimal API 
The `IJobMasterScheduler` is registered in the DI container. You can inject it into your endpoints to trigger background work instantly or at a specific time.

```csharp
    app.MapPost("/schedule-job", async (IJobMasterScheduler jobScheduler) =>
    {
        // Build a fluent message data object
        var msg = WriteableMessageData.New().SetStringValue("Name", "John Doe");
    
        // Enqueue the job for immediate execution
        await jobScheduler.OnceNowAsync<HelloJobHandler>(msg);
        
        return Results.Accepted();
    
    }).WithOpenApi();
```

### Accessing Job Context
The `JobContext` provides metadata and payload data for the current execution:

| Property | Description |
| :--- | :--- |
| `job.Id` | The unique identifier of the job. |
| `job.MsgData` | The data payload (arguments) sent to the job. |
| `job.Metadata` | Non-business data (e.g., correlation IDs, tracking tags). |
---

### Dependency Injection
JobMaster is fully integrated with the .NET Dependency Injection container. You can inject your services (Repositories, HTTP Clients, etc.) directly into the constructor of your Handler.

```csharp
public class NotificationHandler : IJobHandler
{
    private readonly IEmailService _emailService;

    // Services are resolved automatically from the DI container
    public NotificationHandler(IEmailService emailService)
    {
        _emailService = emailService;
    }

    public async Task HandleAsync(JobContext job)
    {
        var email = job.MsgData.GetStringValue("UserEmail");
        await _emailService.SendAsync(email, "Your report is ready!");
    }
}
```

## 📈 Scalling non-standalone configurations
For high-throughput or distributed scenarios, you can define multiple agents and specialized workers.

### Configuration

Register the JobMaster services in your Program.cs. This sets up the cluster identity and the storage provider.
Fluent API is used to configure the JobMaster services.

```csharp
// Program.cs
builder.Services.AddJobMasterCluster(config =>
{
    // Configure the main cluster database
    config.ClusterId("Cluster-1")
          .UsePostgresForMaster("[master-connection-string]");

    // Define agent connections
    config.AddAgentConnectionConfig("Postgres-1")
          .UsePostgresForAgent("[agent-connection-string]");
    
    config.AddAgentConnectionConfig("SqlServer-1")
          .UseSqlServerForAgent("[agent-connection-string]");
    
    config.AddAgentConnectionConfig("Nats-1")
          .UseNatsJetStream("[agent-connection-string]");
    
    /// ... Many more agents as needed
    
    // Attach a worker to a specific connection
    config.AddWorker()
          .AgentConnName("Postgres-1");
    
    config.AddWorker()
          .AgentConnName("SqlServer-1");
     
     config.AddWorker()
          .AgentConnName("Nats-1");
});

// Start the runtime
await app.Services.StartJobMasterRuntimeAsync();
````

## 🏗️ Core Architecture Overview
To achieve horizontal scalling and resilience, JobMaster divides responsibilities into three distinct layers:

### 1. The Cluster Database (Master)
The Source of Truth for the entire ecosystem.

   - **Coordination**: Manages agent registrations and workload distribution.
   - **Persistence**: Stores jobs long-term, providing a full audit trail.
   - **Configuration**: Centralized storage for cluster settings and job definitions.

### 2. Agents (Transport Layer)
Ephemeral storage for jobs ready for immediate execution.
  - **High-Speed Buffering**: Only stores "in-flight" tasks.
  - **Performance Buffering**: New jobs are persisted to Agents first for near-instant execution, then synced asynchronously to the Master.

### Workers (Execution Layer)
The compute power of the system.
    - **Atomic Locks**: Workers claim available jobs using provider-specific atomic operations.
    - **Horizontal Scalling**: Spin up as many worker instances as needed with zero downtime.

Note: A Standalone cluster can be migrated to a Distributed configuration by introducing separate Agents. However, this is a one-way operation. 
Reverting to Standalone mode may result in data loss for jobs currently residing in the Agent's ephemeral transport layer.

## 📅 Recurrence Expressions
JobMaster supports recurrence expressions using the [NaturalCron](https://github.com/hugoj0s3/NaturalCron) library.

```csharp
// FLuent build
var schedule = NaturalCronBuilder.Every(1).Minutes().Build();
jobScheduler.Recurring<HelloJobHandler>(schedule, WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName()), metadata: WritableMetadata.New().SetStringValue("expression", expression), workerLane: lane);

// Via expression string
jobScheduler.Recurring<HelloJobHandler>(NaturalCronExprCompiler.TypeId, "every 1 minutes", WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName()), metadata: WritableMetadata.New().SetStringValue("expression", expression), workerLane: lane);
```

## 📚 Documentation

- **Scheduling**
  - One-off and recurring scheduling, `IJobHandler`, attributes and metadata
  - See: [docs/Scheduling.md](docs/Scheduling.md)

- **Cluster, Agent Connections, and Workers Configuration**
  - Cluster setup, agent connections, workers, lanes, buckets, batch sizing
  - See:
    - [docs/BucketsConfiguration.md](docs/BucketsConfiguration.md)
    - [docs/WorkersConfiguration.md](docs/WorkersConfiguration.md)
    - [docs/AgentsConfiguration.md](docs/AgentsConfiguration.md)
    - [docs/ClusterConfiguration.md](docs/ClusterConfiguration.md)

- **Repositories / Transport Providers**
  - Postgres, MySQL, SQL Server, NATS JetStream
  - See: [docs/Providers.md](docs/Providers.md)

- ** Api
  - [docs/ApiConfiguration.md](docs/ApiConfiguration.md)

## 🐞 Internal Debugging
  - Easy way to see the logs while we don't have UI/Api. Cluster level config.
  ```csharp
  .DebugJsonlFileLogger("[path-to-dir]")
  ```

---

## 🗺️ Roadmap

📖 **See the roadmap:** [docs/Roadmap.md](docs/Roadmap.md)

---



