> [!WARNING]
> **Experimental Alpha Release**
>
> This package is in an early stage and subject to significant changes before 1.0.
> Features and APIs may evolve, and stability is not guaranteed. Not recommended for production environments.

# JobMaster.Postgres
### PostgreSQL storage provider for JobMaster .Net.

This package provides the PostgreSQL implementation for the **JobMaster .Net** engine, supporting both the Master (Coordination) and Agent (Transport) layers.

## 📦 Installation

Install the package via the .NET CLI:

```bash
dotnet add package JobMaster
dotnet add package JobMaster.Postgres
```

### Part 3: Getting Started (Configuration)

## 🚀 Getting Started

To use PostgreSQL as your storage backend, register it during the cluster configuration in your `Program.cs`.

### 1. Configure the Master Database
The Master database acts as the central coordination point for the cluster.

```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("Production-Cluster")
          .UsePostgresForMaster("Your_Connection_String");
});
```

### Configure Agent (Transport)
```csharp
config.AddAgentConnectionConfig("Sql-Transport")
.UsePostgresForAgent("Your_Connection_String");

    // Attach a worker to this transport
    config.AddWorker()
          .AgentConnName("Sql-Transport");
```

### Part 4: Features
## 🛠 Features
* **Atomic Locking:** Utilizes PostgreSQL advisory locks to ensure job execution safety and prevent double-processing across multiple nodes.
* **Auto-Schema Management:** Automatically handles the creation of necessary tables, indexes, and stored procedures on startup.
* **High Throughput:** Optimized for low-latency job claiming and high-frequency status synchronization.

---
**Main Project:** [JobMaster .Net](https://github.com/hugoj0s3/jobmaster-net)  
**License:** MIT
