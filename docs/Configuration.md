# Configuration

This guide explains Cluster, Agent Connections, Worker configuration, buckets, lanes, and batch sizing.

## Cluster setup

```csharp
builder.Services.AddJobMasterCluster(cfg =>
{
    cfg.ClusterId("Cluster-1")
       .UsePostgresForMaster("[master-conn]"); // Postgres example.
});
```

### Cluster-level settings
These global settings define the default behavior for the entire cluster. Most values can be overridden at the Job or Schedule level.

#### `DefaultJobTimeout` (Default: 5 minutes)
The maximum time a single job is allowed to run.
* **Resource Protection:** Prevents "runaway" executions from consuming resources indefinitely.
* **Retry Integration:** Timed-out jobs are marked for retry according to the cluster's retry policy.
* **Override:** Can be overridden at the Job Handler level using the `[JobMasterTimeout]` attribute.

#### `TransientThreshold` (Default: 30 minutes)
The look-ahead window used by background runners to identify near-future jobs for onboarding.
* **Onboarding:** When a job's scheduled time falls within this window, it is moved from the Master to an **[Bucket](#buckets-the-parallel-execution-engine)** for execution.
* **High Throughput:** Large values are better for stable environments, as they reduce the frequency of Master database scans.
* **Dynamic Scaling:** Small values are recommended for environments like Kubernetes, where worker nodes may scale down or restart frequently.

#### `DefaultMaxOfRetryCount` (Default: 3)
The global default for how many times a failing job should be retried before being moved to the `Failed` state.
* **Override:** Can be overridden per job using the `[JobMasterMaxNumberOfRetries]` attribute or dynamically during scheduling.

#### `ClusterMode` (Default: Active)
Governs the operational state and behavior of the entire cluster.
* **Active:** Standard operation mode (scheduling and execution are enabled).
* **Passive:** Maintenance mode. Jobs can be scheduled, but no execution will occur by the workers.
* **Archive:** Read-only mode for historical audit. No new jobs can be scheduled or executed.


#### `MaxMessageByteSize` (Default: 128 KiB)
The upper bound for serialized job payloads used as a pre-dispatch safety check.
* **Guidance:** Set this slightly below your transport's hard limit (e.g., if using NATS with a 1MB limit, set this to ~950 KiB to account for overhead).
* **Flexibility:** A value of `-1` disables application-side enforcement (note: transport providers may still reject oversized messages).

#### `IanaTimeZoneId` (Default: System Local)
The time zone used for interpreting recurring schedules (e.g., "every day at 12pm").
* **Consistency:** By default, it uses the local system time.
* **Distributed Clusters:** If running instances across different time zones, explicitly set this to a specific IANA ID (e.g., `America/Sao_Paulo`) to ensure consistent execution times.

#### `DataRetentionTtl` (Default: 30 days)
Controls the automatic cleanup and data lifecycle of the Master database.
* **Scope:** Affects completed jobs, inactive recurring schedules, and execution logs.
* **Tuning:** Set to `null` to keep history forever. In high-throughput environments, use a lower TTL to prevent the Master database from growing excessively.



## Agent connections

- Define one or more agent connections (Postgres, SQL Server, MySQL, NATS JetStream).
- Agents are the fast, ephemeral transport/storage for in-flight jobs.

```csharp
cfg.AddAgentConnectionConfig("Postgres-1").UsePostgresForAgent("[agent-conn]"); // Postgres example.
```


### Agent Connection Name

The name of the agent connection is used to identify the connection in the cluster. It is also used to identify the connection in the worker configuration.
It needs to be unique within the cluster and not change over time for the connection string, otherwise it will break the cluster.
If you need to change/move the connection creates new connection and set worker to drain the jobs first. 

## Workers

Create one or more workers bound to an agent connection.

```csharp
cfg.AddWorker()
   .AgentConnName("Postgres-1")
   .WorkerName("Worker-A")
   .WorkerLane("Lane1")
   .BucketQtyConfig(JobMasterPriority.Medium, 2)
   .WorkerBatchSize(1000)
   .SetWorkerMode(AgentWorkerMode.Default);
```

- `AgentConnName`: which agent connection this worker consumes.
- `WorkerName`: optional friendly name, if not specified it will the machine name + random suffix.
- `WorkerLane`: logical lane for partitioning (e.g., by tenant/region). Jobs/buckets can be routed by lane.
- `BucketQtyConfig(priority, qty)`: how many execution buckets to run per priority.
- `WorkerBatchSize`: how many jobs sitts as enqueued, how many jobs are transported from master cluster db to agent.



### Buckets: The Parallel Execution Engine

In **JobMaster**, a bucket is the fundamental logical unit of execution and concurrency control. Instead of every worker in the cluster competing for the same global list of jobs, the workload is partitioned into discrete buckets.
Every time that job is close to its execution time, it is moved to a bucket and then executed by a worker.

#### How Buckets Work:
* **Atomic Locking:** A bucket is the smallest unit that a worker can "claim." When a worker claims a bucket, it takes exclusive ownership of all jobs inside it for that execution cycle.
* **Concurrency without Contention:** By partitioning jobs into buckets, JobMaster ensures that multiple workers can process the same Agent Connection simultaneously without overlapping or causing database lock contention.
* **Fault Tolerance:** If a worker node crashes, the lock on its assigned bucket will eventually expire in the Cluster Database. Once the lock is released, another healthy worker can automatically claim the bucket and drain the jobs assigned it back to the Master DB.

#### Configuration: `BucketQtyConfig(priority, qty)`
This setting defines the degree of parallelism for a specific priority level within a worker's configuration.

* **High Quantity:** Use more buckets for high-volume, short-duration tasks (e.g., sending thousands of notifications) to maximize throughput across your worker fleet.
* **Low Quantity:** Use fewer buckets for resource-intensive or long-running tasks (e.g., heavy report generation) to prevent a single worker from overwhelming the system's resources.

> **Pro Tip:** If you have 10 workers and configure 10 buckets for `Priority.Medium`, each worker will likely claim one bucket, providing a perfectly balanced parallel processing stream.
### Agent Worker Modes
The AgentWorkerMode defines the specific responsibility of a node within the JobMaster cluster. By decoupling the "Brain" (Coordination) from the "Muscle" (Execution), you can tailor your infrastructure to handle massive horizontal scale without overloading your databases

#### Standalone
The All-in-One Solution. This is the default mode. It handles the entire job lifecycle:
- Scans the Master Database.
- Onboards jobs to Agent Buckets.
- Executes handlers.

Guidance: If you are unsure which mode to use, start with Standalone. It performs all cluster roles simultaneously.

#### Coordinator
The Brain. In large-scale systems, you typically deploy one or two dedicated Coordinators.

Role:
- Keep the pipeline full by moving jobs from the Master DB to the Agent Buckets.

Benefit:
- Even if execution nodes are at 100% CPU, the system remains responsive and continues to onboard new work on time.

### Execution
The Muscle. These nodes are “Master-Agnostic.”

Role:
- Communicate only with their assigned Agent Connection; they do not poll the Master Database.

Benefit:
- Zero “scanning” load on the Source of Truth, enabling near-infinite horizontal scaling of compute.

#### Drain
The Cluster Janitor. Use this mode when you need to stop a server for updates or scaling down.

Graceful Exit:
- Stops accepting new assignments and finishes current tasks.

Orphan Recovery:
- Identifies orphaned buckets (lost by crashed workers) and redirects their jobs back to the Master DB for re-assignment.

Safety:
- Once all buckets are drained and synced, the process can be safely terminated without data loss.

This matrix explains exactly which internal processes are active in each mode. Use this to design your cluster topology.

| Feature                                  | Standalone | Coordinator | Execution | Drain |
|:-----------------------------------------| :---: | :---: | :---: |:-----:|
| **Scan Master DB** (Look-ahead)          | ✅ | ✅ | ❌ |   ❌   |
| **Onboard to Buckets** (Transient)       | ✅ | ✅ | ❌ |   ❌   |
| **Claim Buckets** (Atomic Lock)          | ✅ | ❌ | ✅ |    ❌    |
| **Execute Handlers** (Compute)           | ✅ | ❌ | ✅ |   ❌   |
| **Sync State to Master** (Audit)         | ✅ | ❌ | ✅ |   ✅   |
| **Accept New Work** (Onboarding)         | ✅ | ✅ | ✅ |   ❌   |
| **Recovery of Orphaned Buckets (Drain)** | ✅ | ✅ |   ❌   | ✅ 

---

# Best Practices & Deployment Recommendations

Follow these guidelines to ensure your JobMaster cluster is cost-effective, performant, and resilient.

## 1. Scaling Strategy
* **Start Small:** Begin with a single **Agent Connection** and 1–2 **Workers**.
* **Scale Horizontally:** As throughput increases, add more nodes in `Execution` mode. This scales your compute power without adding polling pressure to your Master Database.

## 2. Agent Technology Selection
* **Consistency:** Avoid mixing different technologies (e.g., SQL Server and NATS) for the same **Worker Lane**. While JobMaster supports this, it rarely makes sense architecturally and adds complexity. Maybe if want to save money on cloud providers.
* **The "Long-Runner" Rule:** Long-execution jobs (e.g., heavy data processing) fit better in **Database Agent Connections** rather than message brokers.

## 3. Concurrency & Isolation
* **Bucket Sizing:** Tune `BucketQtyConfig` based on your concurrency needs per priority.
    * Increase buckets for high-volume, short-burst tasks.
    * Decrease buckets for resource-heavy tasks to prevent worker exhaustion.
* **Lane Isolation:** Use **Worker Lanes** to isolate workloads that should not compete. Never let a critical path (like "Payment Processing") be delayed by a non-critical path (like "Log Archiving").

## 4. Monitoring & Tuning
* **Continuous Review:** Monitor logs and metrics to identify bottlenecks.
* **Batch Size:** Tune your `WorkerBatchSize` to balance throughput vs. memory usage.
* **Window Settings:** Adjust your `TransientThreshold` based on your infrastructure. Use smaller windows for highly dynamic environments (like Kubernetes) where workers scale or restart frequently.
