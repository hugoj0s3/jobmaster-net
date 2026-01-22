# Repositories / Transport Providers

This guide covers configuring and choosing the transport (agent) provider.

JobMaster separates the Master database (authoritative state) from the Agent transport (in‑flight jobs). Choose the agent by workload characteristics.

## Postgres

- Use when you want transactional semantics with good throughput and simple ops.
- Install:
```bash
dotnet add package JobMaster --version 0.0.2-alpha
dotnet add package JobMaster.Postgres --version 0.0.1-alpha
```
- Setup:
```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("Cluster-1")
          .ClusterTransientThreshold(TimeSpan.FromMinutes(1))
          .ClusterMode(ClusterMode.Active);
    
    config.UsePostgresForMaster("Host=...");

    config.AddAgentConnectionConfig("Postgres-1")
          .UsePostgresForAgent("Host=...");
});
```
- Notes:
  - Ensure sufficient connection pool.
  - Prefer UUID support where available.

## MySQL

- Use when your infra standardizes on MySQL-compatible engines.
- Install:
```bash
dotnet add package JobMaster --version 0.0.2-alpha
dotnet add package JobMaster.MySql --version 0.0.1-alpha
```
- Setup:
```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("Cluster-1")
          .ClusterTransientThreshold(TimeSpan.FromMinutes(1))
          .ClusterMode(ClusterMode.Active);
    
    config.UseMySqlForMaster("Server=...;Database=...;User Id=...;Password=...;TrustServerCertificate=True;");
    
    // Agent connection
    config.AddAgentConnectionConfig("MySql-1")
          .UseMySqlForAgent("Server=...;Database=...;User Id=...;Password=...;TrustServerCertificate=True;");
});
```
- Notes:
  - Tune innodb_buffer_pool_size and connection pooling.
  - Use UseAffectedRows=True
- NuGet: https://www.nuget.org/packages/JobMaster.MySql

## SQL Server

- Use when your environment is Microsoft-first or needs SQL Server features.
- Install:
```bash
dotnet add package JobMaster --version 0.0.2-alpha
dotnet add package JobMaster.SqlServer --version 0.0.1-alpha
```
- Setup:
```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("Cluster-1")
          .ClusterTransientThreshold(TimeSpan.FromMinutes(1))
          .ClusterMode(ClusterMode.Active);
    
    config.UseSqlServerForMaster("Server=...;Database=...;User Id=...;Password=...;TrustServerCertificate=True;");
    
    // Agent connection
    config.AddAgentConnectionConfig("SqlServer-1")
          .UseSqlServerForAgent("Server=...;Database=...;User Id=...;Password=...;TrustServerCertificate=True;");
});
```

- NuGet: https://www.nuget.org/packages/JobMaster.SqlServer


## SQL providers
Any sql provider allow you config the table prefix for the master and agent db by default is "jm_"
Also is possible to skip the table provision for the entire cluster by **.DisableAutoProvisionSqlSchema()**

- Use when you want to implement your own sql database provider.
- Setup:
```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("Cluster-1")
          .ClusterTransientThreshold(TimeSpan.FromMinutes(1))
          .ClusterMode(ClusterMode.Active);
    
    config.UseSqlTablePrefixForMaster("jm_custom_")
          .DisableAutoProvisionSqlSchema();
    
    // Agent connection
    config.AddAgentConnectionConfig("Pg-1")
          .UseSqlTablePrefixForAgent("jm_agent_");
});
```

## NATS JetStream

- Use for ultra-low latency, high fan-out, or ephemeral workloads.
- Install:
```bash
dotnet add package JobMaster --version 0.0.2-alpha
dotnet add package JobMaster.NatJetStream --version 0.0.1-alpha
```
- Setup:
```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("Cluster-1")
          .ClusterTransientThreshold(TimeSpan.FromMinutes(1))
          .ClusterMode(ClusterMode.Active);

    config.AddAgentConnectionConfig("Nats-1")
          .UseNatJetStream("nats://localhost:4222");
});
```
- Notes:
  - Consider stream retention and ack policies.
  - Persistent audit trail still lives in the Master DB.
  - If you decide to use NATS as agent provider you need to TransientThreshold to a lower or equal to 2 minutes.
- NuGet: https://www.nuget.org/packages/JobMaster.NatJetStream

## Choosing a transport

- Prefer RDBMS agents for simpler ops and persistence-heavy flows.
- Prefer JetStream for very low latency and bursty workloads.
- You can mix multiple agent connections and assign workers per connection.
