# <img src="img/logo.svg" width="52" valign="middle" /> JobMaster

**Distributed job orchestration engine for .NET. Built for horizontal scale, designed for resilience.**

[![NuGet (pre)](https://img.shields.io/nuget/vpre/JobMaster?label=JobMaster)](https://www.nuget.org/packages/JobMaster)

📖 **[docs.jobmaster.hugoj0s3.dev](https://docs.jobmaster.hugoj0s3.dev)**

---

## Quick Start

Standalone is the simplest way to run JobMaster. A single database connection handles coordination, job storage, and the transport layer — no additional brokers required.

### Register in Program.cs

```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.UseStandaloneCluster()
          .ClusterId("Local-Cluster-01")
          .UsePostgres("Host=localhost;Database=jobmaster_db;Username=postgres;Password=pwd")
          .AddWorker();
});

var app = builder.Build();

await app.Services.StartJobMasterRuntimeAsync();
```

### Implement a Job Handler

```csharp
public sealed class HelloJobHandler : IJobHandler
{
    public async Task HandleAsync(JobContext job)
    {
        var name = job.MsgData.TryGetStringValue("Name") ?? "World";
        Console.WriteLine($"Hello {name}");
        await Task.CompletedTask;
    }
}
```

Handlers are resolved from the .NET DI container — inject your services (repositories, HTTP clients, etc.) directly into the constructor.

### Schedule a Job

`IJobMasterScheduler` is registered automatically. Inject it anywhere in your application.

```csharp
app.MapPost("/schedule-job", async (IJobMasterScheduler jobScheduler) =>
{
    var msg = WriteableMessageData.New().SetStringValue("Name", "John Doe");

    await jobScheduler.OnceNowAsync<HelloJobHandler>(msg);

    return Results.Accepted();
}).WithOpenApi();
```

---

## Core Concepts

JobMaster separates responsibilities into three layers:

- **Cluster Database (Master)** — source of truth. Stores jobs, coordinates agents, and persists configuration.
- **Agents (Transport Layer)** — ephemeral, high-speed buffers for in-flight jobs. Supports PostgreSQL, MySQL, SQL Server, and NATS JetStream.
- **Workers (Execution Layer)** — claim and execute jobs using atomic locks. Scale horizontally with zero downtime.

---

## Recurring Schedules

JobMaster supports recurrence expressions using the [NaturalCron](https://github.com/hugoj0s3/NaturalCron) library.

```csharp
// Fluent builder
var expression = NaturalCronBuilder.Every(1).Minutes().Build();
await jobScheduler.RecurringAsync<HelloJobHandler>(expression);

// Expression string
await jobScheduler.RecurringAsync<HelloJobHandler>(NaturalCronExprCompiler.TypeId, "every 1 minutes");
```

---

## Dashboard & API

JobMaster ships a browser-based dashboard and a REST API for monitoring clusters, jobs, workers, buckets, and agent connections in real time.

```bash
dotnet add package JobMaster.Api
dotnet add package JobMaster.Dashboard
```

Both can run in a completely separate process from your workers — all they need is access to the master database.

---

## Documentation

Full documentation is available at **[docs.jobmaster.hugoj0s3.dev](https://docs.jobmaster.hugoj0s3.dev)**:

- [Getting Started](https://docs.jobmaster.hugoj0s3.dev/docs/getting-started/getting-started)
- [Architecture Overview](https://docs.jobmaster.hugoj0s3.dev/docs/core-concepts/architecture-overview)
- [Scaling Up](https://docs.jobmaster.hugoj0s3.dev/docs/advanced/scalling-up-introduction)
- [Providers](https://docs.jobmaster.hugoj0s3.dev/docs/advanced/providers)
- [Recurring Schedules](https://docs.jobmaster.hugoj0s3.dev/docs/scheduling/recurring-schedule)
- [Dashboard](https://docs.jobmaster.hugoj0s3.dev/docs/dashboard/configuration)
- [API](https://docs.jobmaster.hugoj0s3.dev/docs/api/api-configuration)

---

## Roadmap

See [docs.jobmaster.hugoj0s3.dev/docs/roadmap](https://docs.jobmaster.hugoj0s3.dev/docs/roadmap).
