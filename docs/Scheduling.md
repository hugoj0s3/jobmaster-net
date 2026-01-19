# Scheduling

This guide covers how to implement job handlers, schedule one-off and recurring tasks, and configure job behavior using attributes or dynamic parameters.

## Implementing a job Handler
To process work, you must implement the `IJobHandler` interface. Handlers are the entry point for your business logic. It uses IServiceProvider to resolve dependencies so all underline dependencies should be registered in the DI container.
```csharp
public sealed class HelloJobHandler : IJobHandler
{
    public Task HandleAsync(JobContext job)
    {
        var name = ctx.MsgData.TryGetStringValue("Name") ?? "World";
        Console.WriteLine($"Hello {name}");
        await Task.CompletedTask;
    }
}
```

## One-off jobs
Use the IJobMasterScheduler to schedule jobs. You can enqueue work for immediate execution or for a specific time in the future.
We have async and sync methods for all scheduling options.

```csharp
// 1. Schedule immediately
var msg = WriteableMessageData.New()
    .SetStringValue("Name", "John Doe")
    .SetIntValue("Tries", 1);

await jobMasterScheduler.OnceNowAsync<HelloJobHandler>(msg);

// 2. Schedule for a specific DateTime 
var runAt = DateTime.UtcNow.AddMinutes(5);
await jobMasterScheduler.OnceAtAsync<HelloJobHandler>(runAt, msg);

// 3. Schedule using a relative time in the future (TimeSpan)
await jobMasterScheduler.OnceAfterAsync<HelloJobHandler>(TimeSpan.FromMinutes(5), msg);
```

## Job Configurations
You can control how the cluster treats your jobs by using attributes on the handler class.

### JobDefinitionId
Defines the unique identity of the job. By default, it uses the Type FullName. 
Recommendation: Always define a custom ID to avoid breaking changes if you rename the handler class or move it to a different namespace.
```csharp
[JobMasterDefinitionId("HelloJob")]
public sealed class HelloJobHandler : IJobHandler
```
### Job Timeout
Defines the job timeout. If not specified, it will use the default timeout from the cluster configuration.
```csharp
[JobMasterTimeout(10)]
public sealed class HelloJobHandler : IJobHandler
```

### Max Retries
Defines how many times the system should attempt to re-run the job if it fails.
```csharp
[JobMasterMaxNumberOfRetries(3)]
public sealed class HelloJobHandler : IJobHandler
```

### Job Priority 
Influences the order of execution. If not specified, it defaults to Medium.
```csharp
[JobMasterPriority(JobMasterPriority.Low)]
public sealed class HelloJobHandler : IJobHandler
```

### Worker Lane 
Worker lanes allow you to isolate workloads. 
You must configure specific workers to only process jobs from a specific lane. This is useful for long-running jobs to prevent them from blocking shorter, very high-priority tasks.
```csharp
[JobMasterWorkerLane("PaymentsProcessing")]
public sealed class HelloJobHandler : IJobHandler
```

### Metadata
Allows you to attach extra information to a job for categorization, auditing, or custom logic.
```csharp
[JobMasterMetadata("Category", "Payroll")]
public sealed class HelloJobHandler : IJobHandler
```

### Adding/Overriding Configuration at Runtime
The IJobMasterScheduler allows you to override or add any attribute-defined configuration on the fly when enqueuing a job.

```csharp
// Overriding priority and metadata during scheduling
var customMeta = WritableMetadata.New().SetStringValue("Source", "WebAPI");

await scheduler.OnceNowAsync<HelloJobHandler>(
    msg, 
    priority: JobMasterPriority.Low, 
    metadata: customMeta
);
```

Multi-Cluster Support
If your application connects to multiple JobMaster clusters, you can specify which one to use by providing the clusterId.

```csharp
await scheduler.OnceNowAsync<HelloJobHandler>(clusterId: "sales-microservice-cluster");
```



## Recurring schedules
JobMaster provides a flexible system for recurring tasks. You can define schedules using time intervals, Cron expressions, or even natural language.

### Dynamic Recurring Jobs
Dynamic schedules are tied to specific data (e.g., a specific subscription renewal or a per-user cleanup task).
```csharp
// Using the built-in TimeSpanInterval provider (runs every 5 minutes)
await scheduler.RecurringAsync<HelloJobHandler>(TimeSpan.FromMinutes(5));

// Passing specific data to a recurring job
var data = WriteableMessageData.New().SetStringValue("SubscriptionId", "sub_123");
await scheduler.RecurringAsync<RenewalHandler>(
    NaturalCronExprCompiler.TypeId, 
    "every year", 
    data: data
);
```
Note: We currently support TimeSpanIntervalExprCompiler. Support for CronExprCompiler and NaturalCronExprCompiler is coming soon as optional extensions.
 
### Static Recurring Profiles (System Jobs)
For system-wide routines like backups or maintenance, you can define "Static" profiles. These do not transport message data and are typically used for global background tasks.

```csharp
public class MaintenanceProfile : StaticRecurringSchedulesProfile
{
    public override void Configure(RecurringScheduleDefinitionCollection schedules)
    {
        schedules
            // Automatically ID: cluster:maint:CleanupHandler
            .Add<CleanupHandler>(TimeSpan.FromDays(1)) 
            
            // Explicit ID: cluster:maint:HourlySync
            .Add<SyncHandler>(TimeSpan.FromHours(1), defId: "HourlySync"); 
    }
}
```

### Technical Comparison: Dynamic vs. Static Recurring Jobs

| Feature | Dynamic Recurring | Static Profile |
| :--- | :--- | :--- |
| **Primary Use Case** | Per-entity logic (e.g., specific Subscription, User cleanup). | Global system routines (e.g., Database backup, Log rotation). |
| **Data Payload** | **Supported.** Can transport unique `MsgData` for each instance. | **Not Supported.** Handlers run without specific message data. |
| **Where to Define** | Enqueued at runtime via `IJobMasterScheduler`. | Defined in code by implementing `IStaticRecurringSchedulesProfile`. |
| **Persistence** | Stored and managed permanently in the Cluster Database. | Stored in the Cluster DB for monitoring, but automatically **inactivated** if the profile is removed from code. |
| **Scalability** | Can be created/deleted dynamically by your business logic. | Fixed at deployment time; requires a code change or profile update to modify. |

### Recurring Schedule Configuration
Configuring a recurring schedule works exactly like a one-off job. You can specify:
* **Priority, Worker Lane, Timeout, and Max Retries.**
* **Metadata and ClusterId.**

**Important: Configuration Overrides**
The configuration provided within the `RecurringScheduleProfile` (or via the dynamic `RecurringAsync` call) **overrides** any attributes defined on the `IJobHandler` class. This allows you to reuse the same handler across different schedules with different execution priorities or worker lanes.

## Best Practices

To ensure your cluster remains healthy and your jobs are processed reliably, follow these architectural guidelines:

### 1. Define a Static DefinitionId
Always use the `[JobMasterDefinitionId]` attribute. By default, JobMaster uses the class's full name. If you rename the class or change its namespace without a static ID, the cluster will not be able to map existing persisted jobs to the new code, leading to execution failures.

### 2. Design for Idempotency
In a distributed system, the "exactly-once" delivery guarantee is a myth at scale. Network flickers or node failures might cause a job to be retried even if it partially succeeded. Ensure your `HandleAsync` logic is safe to run multiple times (e.g., check if a payment was already processed before charging again).

### 3. Keep Payloads Lean
Message data is limited to **128 KB** by default.
* **Bad:** Passing a large JSON string or a byte array of an image in the job data.
* **Good:** Uploading the file to S3 or a database and passing only the `FileId` or `URL` in the `MsgData`. This keeps the Agent storage fast and reduces I/O overhead.

### 4. Leverage Worker Lanes
Don't let a few "heavy" jobs (like generating a 500-page PDF) starve your "light" jobs (like sending a welcome email). Use **Worker Lanes** to isolate long-running or resource-intensive tasks to a dedicated pool of workers.

### 5. High-Frequency Recurring Schedules
Avoid using **secondly** schedules (e.g., every 1 or 5 seconds) unless your business logic can tolerate significant jitter.
* **The Reality:** In a distributed cluster, polling intervals and network latency can cause a 10-20 second delay.
* **Overlap Risk:** If a job takes longer to run than its schedule interval, you may experience overlapping executions. For high-precision, sub-minute tasks, consider using a dedicated streaming solution like NATS JetStream directly.

---

