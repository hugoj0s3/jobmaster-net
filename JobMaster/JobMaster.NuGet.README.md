> [!WARNING]
> **Experimental Alpha Release**
>
> This package is in an early stage and subject to significant changes before 1.0.
> Features and APIs may evolve, and stability is not guaranteed. Not recommended for production environments.

# JobMaster .Net
## Distributed job orchestration engine for .NET. Built for horizontal scale.

JobMaster is a high-performance framework designed to manage and execute background tasks across a distributed cluster. By decoupling coordination from execution, it allows developers to scale compute resources horizontally based on real-time workload demands without compromising system stability.

## 📦 Installation & NuGet

```bash
dotnet add package JobMaster --version 0.0.2-alpha
```


## 🏗 Core Architecture

To achieve true resilience and massive scale, JobMaster utilizes a **three-layer architecture**:

1.  **The Master (Coordination):** The "Source of Truth." It manages cluster topology, job definitions, and long-term auditing. It ensures that the state of your entire ecosystem is persistent and consistent.
2.  **Agents (Transport):** High-speed ephemeral storage or message brokers (PostgreSQL, SQL Server, or NATS JetStream). Agents act as high-performance buffers for "in-flight" jobs, ensuring workers can claim tasks with ultra-low latency.
3.  **Workers (Execution):** The compute power. Workers monitor specific Agents, claim available jobs using atomic locks, and synchronize execution results back to the Master Database.

## 🚀 Getting Started

### 1. Configuration
Register JobMaster in your `Program.cs`. This defines your cluster identity and sets up your storage providers.

```csharp
builder.Services.AddJobMasterCluster(config =>
{
    // Define the central coordination database
    config.ClusterId("Production-Cluster")
          .UsePostgresForMaster(connectionString);

    // Define the transport layer (Agent)
    config.AddAgentConnectionConfig("Transport-1")
          .UsePostgresForAgent(agentConnectionString);
    
    // Attach a worker to the transport
    config.AddWorker()
          .AgentConnName("Transport-1");
});

// Start the runtime
await app.Services.StartJobMasterRuntimeAsync();
```

### Implement a Job Handler
Define your logic by implementing IJobHandler
```csharp
public class NotificationHandler : IJobHandler
{
    public async Task ExecuteAsync(JobContext job)
    {
        var userId = job.MsgData.GetStringValue("UserId");
        // Business logic here...
        await Task.CompletedTask;
    }
}
```

### Schedule Jobs
Inject IJobMasterScheduler into your services or Minimal APIs.

Immediate or Delayed Execution

```csharp
app.MapPost("/send-welcome", async (IJobMasterScheduler scheduler) =>
{
    var msg = WriteableMessageData.New().SetStringValue("UserId", "user_123");
    
    // Enqueue for immediate processing
    await scheduler.OnceNowAsync<NotificationHandler>(msg);
    
    return Results.Accepted();
});
```

### Recurring Schedules (Static & Dynamic)
JobMaster supports both code-defined (Static) and runtime-defined (Dynamic) recurring tasks.

```csharp
var msg = WriteableMessageData.New().SetStringValue("UserId", "user_123");

await scheduler.RecurringAsync<NotificationHandler>(
    TimeSpan.FromHours(24), 
    msg);
```

## 🛠 Advanced Features

* **Transport Agnostic:** Seamlessly switch between RDBMS (Postgres, MySQL, SQL Server) and Message Brokers (NATS JetStream) without changing your business logic.
* **Performance Buffering:** New jobs are instantly persisted to the Agent for immediate execution, then asynchronously synced to the Master for long-term auditing.
* **Atomic Locking:** Built-in protection to ensure that even in a multi-node cluster, a job is never executed by more than one worker simultaneously.
* **Static & Dynamic Scheduling:** Support for code-defined "Static Profiles" that sync on startup and "Dynamic Schedules" created at runtime via API.
* **Horizontal Scaling:** Add or remove worker instances on the fly to handle traffic spikes without reconfiguring your Master database.
