> [!WARNING]
> **Experimental Alpha Release**
>
> This package is in an early stage and subject to significant changes before 1.0.
> Features and APIs may evolve, and stability is not guaranteed. Not recommended for production environments.

# JobMaster.SqlServer
### SQL Server storage provider for JobMaster .Net.

This package provides the SQL Server implementation for the **JobMaster .Net** engine, supporting both the Master (Coordination) and Agent (Transport) layers.

## 📦 Installation

Install the package via the .NET CLI:

```bash
dotnet add package JobMaster
dotnet add package JobMaster.SqlServer
```

### Part 3: Getting Started (Configuration)

## 🚀 Getting Started

To use SQL Server as your storage backend, register it during the cluster configuration in your `Program.cs`.

### 1. Configure the Master Database
The Master database acts as the central coordination point for the cluster.

```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("Production-Cluster")
          .UseSqlServerForMaster("Your_Connection_String");
});
```

### Configure Agent (Transport)
```csharp
config.AddAgentConnectionConfig("Sql-Transport")
.UseSqlServerForAgent("Your_Connection_String");

    // Attach a worker to this transport
    config.AddWorker()
          .AgentConnName("Sql-Transport");
```

### Part 4: Database Setup Recommendation

## ⚡ Enable READ_COMMITTED_SNAPSHOT (Recommended)

JobMaster relies on non-blocking reads for counts, queries, and heartbeats. On SQL Server, the safest way to achieve this is to enable **READ_COMMITTED_SNAPSHOT isolation (RCSI)** at the database level:

```sql
ALTER DATABASE YourDatabaseName SET READ_COMMITTED_SNAPSHOT ON;
```

With RCSI enabled, `READ COMMITTED` queries use row-version snapshots instead of shared locks — readers never block writers and writers never block readers. This significantly reduces contention under concurrent workloads without any application-level changes.

> **Note:** RCSI requires brief exclusive database access to apply. Run during a maintenance window on production databases.

### Part 5: Features
## 🛠 Features
* **Atomic Locking:** Utilizes SQL Server application locks to ensure job execution safety and prevent double-processing across multiple nodes.
* **Auto-Schema Management:** Automatically handles the creation of necessary tables, indexes, and stored procedures on startup.
* **High Throughput:** Optimized for low-latency job claiming and high-frequency status synchronization.

---
**Main Project:** [JobMaster .Net](https://github.com/hugoj0s3/jobmaster-net)  
**License:** MIT