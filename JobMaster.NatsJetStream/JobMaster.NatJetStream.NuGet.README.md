> [!WARNING]
> **Experimental Alpha Release**
>
> This package is in an early stage and subject to significant changes before 1.0.
> Features and APIs may evolve, and stability is not guaranteed. Not recommended for production environments.

# JobMaster.NatJetStream
### NATS JetStream transport provider for JobMaster .Net.

This package provides the NATS JetStream implementation for the **JobMaster .Net** engine's **Agent (Transport) layer only**. 

> **Note:** NATS JetStream can only be used as a transport layer. You must use a database provider (PostgreSQL, SQL Server, or MySQL) for the Master (Coordination) layer.

## 📦 Installation

Install the package via the .NET CLI:

```bash
dotnet add package JobMaster
dotnet add package JobMaster.NatJetStream
```

## 🚀 Getting Started

To use NATS JetStream as your transport backend, you need to configure both a database for the Master and NATS JetStream for the Agent.

### Configuration Example

```csharp
builder.Services.AddJobMasterCluster(config =>
{
    // Master MUST use a database provider (Postgres, SQL Server, or MySQL)
    config.ClusterId("Production-Cluster")
          .UsePostgresForMaster("Your_Database_Connection_String");

    // Agent can use NATS JetStream for high-performance message transport
    config.AddAgentConnectionConfig("NATS-Transport")
          .UseNatsJetStreamForAgent("nats://localhost:4222");
    
    // Attach a worker to the NATS transport
    config.AddWorker()
          .AgentConnName("NATS-Transport");
});
```

### Part 4: Features
## 🛠 Features
* **High-Performance Messaging:** Leverages NATS JetStream's ultra-low latency message delivery for rapid job distribution.
* **Stream Persistence:** Jobs are persisted in JetStream streams, providing durability and replay capabilities.
* **Scalable Transport:** Ideal for high-throughput scenarios where jobs need to be distributed quickly across many workers.
* **Consumer Groups:** Supports NATS consumer groups for load balancing job consumption across multiple worker instances.

---
**Main Project:** [JobMaster .Net](https://github.com/hugoj0s3/jobmaster-net)  
**License:** MIT
