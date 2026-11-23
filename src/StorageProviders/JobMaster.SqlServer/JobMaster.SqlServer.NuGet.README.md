> [!WARNING]
> **Experimental Alpha Release**
>
> This package is in an early stage and subject to significant changes before 1.0.
> Features and APIs may evolve, and stability is not guaranteed. Not recommended for production environments.

# JobMaster.SqlServer
### SQL Server storage provider for JobMaster .Net.

📚 [Documentation](https://docs.jobmaster.hugoj0s3.dev)

This package provides the SQL Server implementation for the **JobMaster .Net** engine, supporting both the Master (Coordination) and Agent (Transport) layers.

## 📦 Installation

Install the package via the .NET CLI:

```bash
dotnet add package JobMaster
dotnet add package JobMaster.SqlServer
```

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

## ⚡ Enable READ_COMMITTED_SNAPSHOT (Recommended)

JobMaster relies on non-blocking reads for counts, queries, and heartbeats. On SQL Server, the safest way to achieve this is to enable **READ_COMMITTED_SNAPSHOT isolation (RCSI)** at the database level:

```sql
ALTER DATABASE YourDatabaseName SET READ_COMMITTED_SNAPSHOT ON;
```

With RCSI enabled, `READ COMMITTED` queries use row-version snapshots instead of shared locks — readers never block writers and writers never block readers. This significantly reduces contention under concurrent workloads without any application-level changes.

> **Note:** RCSI requires brief exclusive database access to apply. Run during a maintenance window on production databases.

## 🛠 Features
* **Distributed Locking:** Coordinates the nodes of a cluster through a lock table in the database. Locks expire on their own, so a node that crashes can't hold one forever.
* **Auto-Schema Management:** Automatically handles the creation of necessary tables and indexes on startup.
* **High Throughput:** Optimized for low-latency job claiming and high-frequency status synchronization.

---
**Main Project:** [JobMaster .Net](https://github.com/hugoj0s3/jobmaster-net)  
**Docs:** [SQL Server provider guide](https://docs.jobmaster.hugoj0s3.dev/docs/configuration/providers/sql-server)  
**License:** MIT