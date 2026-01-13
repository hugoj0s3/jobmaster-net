# Repositories / Transport Providers

This guide covers configuring and choosing the transport (agent) provider.

JobMaster separates the Master database (authoritative state) from the Agent transport (in‑flight jobs). Choose the agent by workload characteristics.

## Postgres

- Use when you want transactional semantics with good throughput and simple ops.
- Setup:
```csharp
cfg.AddAgentConnectionConfig("Postgres-1")
   .UsePostgresForAgent("Host=...;Username=...;Password=...;Database=...");
```
- Notes:
  - Ensure sufficient connection pool.
  - Prefer UUID/JSONB support where available.

## MySQL

- Use when your infra standardizes on MySQL-compatible engines.
- Setup:
```csharp
cfg.AddAgentConnectionConfig("MySql-1")
   .UseMySqlForAgent("Server=...;Uid=...;Pwd=...;Database=...;");
```
- Notes:
  - Tune innodb_buffer_pool_size and connection pooling.
  - Use UseAffectedRows=True

## SQL Server

- Use when your environment is Microsoft-first or needs SQL Server features.
- Setup:
```csharp
cfg.AddAgentConnectionConfig("SqlServer-1")
   .UseSqlServerForAgent("Server=...;Database=...;User Id=...;Password=...;TrustServerCertificate=True;");
```
- Notes:
  - Align isolation levels with your ops policy.

## NATS JetStream

- Use for ultra-low latency, high fan-out, or ephemeral workloads.
- Setup:
```csharp
cfg.AddAgentConnectionConfig("Nats-1")
   .UseNatsJetStreamForAgent("nats://localhost:4222");
```
- Notes:
  - Consider stream retention and ack policies.
  - Persistent audit trail still lives in the Master DB.
  - If you decide to use NATS as agent provider you need to TransientThreshold to a lower or equal to 2 minutes.

## Choosing a transport

- Prefer RDBMS agents for simpler ops and persistence-heavy flows.
- Prefer JetStream for very low latency and bursty workloads.
- You can mix multiple agent connections and assign workers per connection.
