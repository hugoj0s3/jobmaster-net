# Agent Connections
While the Cluster defines global policy, the Agent Connection and Worker Configuration define how individual instances interact with the hardware and database.

## Agent Connection Strings
An Agent is a high-speed persistence layer (PostgreSQL/SQL Server/MySQL/NATS JetStream) used by workers to manage their buckets. 
You can define multiple connections to distribute database load even mix different types of agents.

```csharp
config.AddAgentConnectionConfig("Postgres-1")
    .UsePostgresForAgent("Host=localhost;Database=agent_1;...");
```

**CRITICAL**: The Immutable Name
The agent connection name (e.g., Postgres-1) is the unique identifier used to bind workers to their persistence layer.

**Cluster Uniqueness**: It must be unique across the entire cluster.

**Immutability**: Once established and jobs are processed, this name must never be changed.

### Connection Protection

By default, JobMaster guards against accidental misconfiguration by enabling connection protection on every Agent Connection.

```csharp
config.AddAgentConnectionConfig("Postgres-1")
    .UsePostgresForAgent(connectionString)
    .ProtectConnectionChanges(true); // default — can be omitted
```

**What it does:**

- **Startup guard:** If the connection's stored footprint differs from the current configuration (e.g. the connection string was silently changed to point at a different database), JobMaster will refuse to start and throw an exception. With `false`, it logs a warning and continues.
- **Runtime alert:** If a protected connection goes silent (no heartbeat for more than 10 minutes) while it still owns active buckets, a **Critical** log is emitted warning that jobs may be lost if the connection is not restored.

Set to `false` only in development or when intentionally migrating to a new database instance and losing in-flight jobs is acceptable. For production, leave this at its default and follow the [Safe Migration Strategy](#safe-migration-strategy) instead.

## Safe Migration Strategy
If you need to migrate to a different agent type (e.g., moving from Postgres to NATS JetStream) or move to a new database instance, follow this protocol to ensure no jobs are lost:

**Create New Connection**: Add the new Agent connection with a new unique name (e.g., Postgres-New).

**Drain the Old Connection**: Set the old connection/workers to Drain Mode. This tells the system to stop onboarding new jobs to those specific buckets and finish the existing work.

**Attach New Workers**: Attach your new workers to Postgres-New. They will begin handling all new incoming SavePending and AssignedToBucket jobs.

**Decommission**: Only remove the old connection from your configuration once the old buckets are confirmed empty and their status moves to ReadyToDelete.

## Producer-Consumer Configuration
JobMaster is designed for high-scale environments where you may want to separate the Producer (Web/API) from the Consumer (Worker Services) across different servers or containers.


### Producer-Only Instances (API/Web)
To enable an instance to only schedule work, define the connection but omit the worker call. This allows your API to scale horizontally without the resource overhead of background processing.
```csharp
config.AddAgentConnectionConfig("Postgres-1") // Must match the Worker!
    .UsePostgresForAgent(connectionString);
    
// Note: No .AddWorker() is called here
```

#### Consumer-Only Instances (Workers)

To enable an instance to only process work, define the connection and bind a worker to it.

```csharp
config.AddAgentConnectionConfig("Postgres-1") // Must match the Producer!
    .UsePostgresForAgent(connectionString);

config.AddWorker()
    .AgentConnName("Postgres-1"); // Binds this worker to that specific 'address'
```
### Scaling & The Hand-off
This separation allows for independent scaling of your infrastructure. You can have 10 API instances handing off work to 50 dedicated Worker instances.

**Immediate Job Flow**: When you schedule a job using SavePending:
1. The Producer writes the job to the **Agent ephemeral transport** — the Master DB is not touched on the hot path.
2. A background runner evaluates the job's planned start time against the `TransientThreshold`:
   - **Within the window (YES path):** The job is written to the Master DB for auditing and routed directly to an active bucket (`AssignedToBucket`) for near-immediate execution.
   - **Outside the window (NO path):** The job is flushed to the Master DB as `HeldOnMaster` and picked up later by the Coordinator scan.


See: [ClusterConfiguration](ClusterConfiguration.md)


