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
.AgentConnName("Postgres-1") // Binds this worker to that specific 'address'
```
### Scaling & The Hand-off
This separation allows for independent scaling of your infrastructure. You can have 10 API instances handing off work to 50 dedicated Worker instances.

**Immediate Job Flow**: When you schedule a job for immediate execution:
1. The Producer saves the job into the Master DB. 
2. The job bypasses the `HeldOnMaster` status if it is scheduled within the `TransientThreshold` window.
3. It is automatically set to `AssignedToBucket`, allowing the next available Agent Worker to pick it up and move it to Queued within milliseconds.

