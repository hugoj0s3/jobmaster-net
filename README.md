# JobMaster .Net
## Distributed job orchestration engine for .NET. Oriented to horizontal scaling and flexibility.

JobMaster is a framework designed to manage and execute background tasks across a distributed cluster. By decoupling coordination from execution, it allows developers to scale their infrastructure horizontally based on workload demands.

[![NuGet (pre)](https://img.shields.io/nuget/vpre/JobMaster?label=JobMaster)](https://www.nuget.org/packages/JobMaster)

## Overview
JobMaster provides a architecture to handle job lifecycles. It is built to be transport-agnostic, supporting RDBMS (PostgreSQL, SQL Server, MySQL) and Message Brokers (NATS JetStream).

### Getting Started

#### Step 1: Configuration

Register the JobMaster services in your Program.cs. This sets up the cluster identity and the storage provider.
Fluent API is used to configure the JobMaster services.

```csharp
// Program.cs
builder.Services.AddJobMasterCluster(config =>
{
    // Configure the main cluster database
    config.ClusterId("Cluster-1")
          .UsePostgresForMaster("[master-connection-string]");

    // Define agent storage connections
    config.AddAgentConnectionConfig("Postgres-1")
          .UsePostgresForAgent("[agent-connection-string]");
    
    // Attach a worker to a specific connection
    config.AddWorker()
          .AgentConnName("Postgres-1");
});

// Start the runtime
await app.Services.StartJobMasterRuntimeAsync();
```

#### Step 2: Implementing a Job Handler
A Job Handler contains the logic that will be executed by the workers.
```csharp
public class HelloJobHandler : IJobHandler
{
    public async Task HandleAsync(JobContext job)
    {
        var name = job.MsgData.GetStringValue("Name");
        Console.WriteLine($"Hello {name}");
    
        await Task.CompletedTask;
    }
}
```

#### Step 3: Schedule from a Minimal API
Inject IJobMasterScheduler into your endpoints to trigger background work instantly or at a specific time.

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

### Core Architecture Concepts Overview
To achieve true horizontal scaling and resilience, JobMaster divides responsibilities into three distinct layers:

#### The Cluster Database (Master)
The Cluster Database is the Source of Truth for the entire ecosystem.

**Coordination**: It manages agent registrations and coordinates workload distribution across the cluster.

**Persistence**: It stores jobs permanently or for long periods, providing a full audit trail of execution history.

**Configuration**: Centralized storage for cluster-wide settings and job definitions.

#### Agents (Transport Layer)

Agents act as the Ephemeral Storage (or Transport) for jobs that are ready for immediate or near-future execution.

**High-Speed Buffering**: Agents only store tasks that are "in-flight" or waiting for a worker to pick them up.

**Versatility**: An agent can be a database (Postgres/SQL Server) for persistence-heavy tasks or a message broker (NATS JetStream) for ultra-low latency scenarios.

**Transient Nature**: Once a job is completed or moved back to the Master, its record in the Agent is typically cleared.

**Performance Buffering (Save Pending)**: New jobs are initially persisted directly into the Agent storage to allow for near-instant execution and better throughput. The system then asynchronously synchronizes these records back to the Master Database for long-term persistence.

#### Workers (Execution Layer)
Workers are the Compute Power of the system.

**Job Execution**: They monitor specific Agents, claim available jobs using atomic locks, and run the handler logic.

**State Synchronization:** Workers communicate with the Master Database to update job statuses (Succeeded, Failed, Retrying) and persist execution logs.

**Horizontal Scaling**: You can spin up as many worker instances as needed to handle your current workload with no downtime.

### Recurrence Expressions

JobMaster supports recurrence expressions using the [NaturalCron](https://github.com/hugoj0s3/NaturalCron) library.

```csharp
// FLuent build
var schedule = NaturalCronBuilder.Every(1).Minutes().Build();
jobScheduler.Recurring<HelloJobHandler>(schedule, WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName()), metadata: WritableMetadata.New().SetStringValue("expression", expression), workerLane: lane);

// Via expression string
jobScheduler.Recurring<HelloJobHandler>(NaturalCronExprCompiler.TypeId, "every 1 minutes", WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName()), metadata: WritableMetadata.New().SetStringValue("expression", expression), workerLane: lane);
```

## Documentation

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

- **Internal Debugging**
  - Easy way to see the logs while we don't have UI/Api. Cluster level config.
  ```csharp
  .DebugJsonlFileLogger("[path-to-dir]")
  ```



