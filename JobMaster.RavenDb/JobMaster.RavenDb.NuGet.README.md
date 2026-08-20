> [!WARNING]
> **Experimental Alpha Release**
>
> This package is in an early stage and subject to significant changes before 1.0.
> Features and APIs may evolve, and stability is not guaranteed. Not recommended for production environments.

# JobMaster.RavenDb
### RavenDB storage provider for JobMaster .Net.

This package provides the RavenDB implementation for the **JobMaster .Net** engine, supporting both the Master (Coordination) and Agent (Transport) layers.

## 📦 Installation

Install the package via the .NET CLI:

```bash
dotnet add package JobMaster
dotnet add package JobMaster.RavenDb
```

## 🚀 Getting Started

To use RavenDB as your storage backend, register it during the cluster configuration in your `Program.cs`.

### 1. Configure the Master Database
The Master database acts as the central coordination point for the cluster.

```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("Production-Cluster")
          .UseRavenDb("Urls=http://localhost:8080;Database=JobMaster");
});
```

### Configure Agent (Transport)
```csharp
config.AddAgentConnectionConfig("RavenDb-Transport")
      .UseRavenDb("Urls=http://localhost:8080;Database=JobMaster");

    // Attach a worker to this transport
    config.AddWorker()
          .AgentConnName("RavenDb-Transport");
```

### Client Certificate Authentication
For secured RavenDB clusters, pass an already-loaded client certificate:

```csharp
config.UseRavenDb(
    "Urls=https://your-server:8080;Database=JobMaster",
    certificate: new X509Certificate2("client.pfx", "password"));
```

### Part 4: Features
## 🛠 Features
* **Compare-Exchange Locking:** Uses RavenDB's cluster-wide compare-exchange primitive for atomic distributed locking, with a periodic backstop that cleans up locks left behind by a crashed worker.
* **Static Index Acceleration:** Deploys dedicated static indexes for the job-claiming hot path, avoiding the overhead of RavenDB's dynamic auto-indexing there.
* **Optional Document Expiration:** Can opt in to RavenDB's native document-expiration background job for extra storage housekeeping, on a configurable schedule.
* **Client Certificate Support:** Supports RavenDB's X.509 client-certificate authentication for secured clusters.

---
**Main Project:** [JobMaster .Net](https://github.com/hugoj0s3/jobmaster-net)  
**License:** MIT
