# Workers Configuration
The Worker is the engine's "Muscle." It represents an execution unit that maps to a specific Agent Connection and defines how jobs are fetched, prioritized, and executed.

## Defining a Worker
A single application instance can host multiple workers, each potentially pointing to different Agent connections or operating in different Lanes.

```csharp
builder.Services.AddJobMasterCluster(config =>
{
config.ClusterId("My-Cluster");

    config.AddWorker()
        .AgentConnName("Postgres-1")         // Links to an Agent connection
        .WorkerName("Payroll-Worker-01")    // Unique name for this instance
        .WorkerLane("Payroll")           // Logical isolation lane
        .TransferBatchSize(1000)            // Jobs pulled per DB round-trip for bucket transfers and other bulk operations
        .BucketBufferSize(250)              // Max jobs held in memory per bucket awaiting execution
        .BucketBufferLeadTime(TimeSpan.FromSeconds(30)) // How far ahead to look when filling the in-memory buffer (max 30s)
        .ParallelismFactor(2)               // Scaler for concurrent execution
        .SetWorkerMode(AgentWorkerMode.Full)
        .BucketQtyConfig(JobMasterPriority.Critical, 3);
});
```

### Worker Name

`.WorkerName()` is optional. The behaviour depends on whether you supply a value.

**Not provided — auto-generated from hostname**

```
{hostname}-{timestampId}
```

Example: `myserver-3c1a8b2`

The timestamp suffix guarantees uniqueness across process restarts on the same host.

> [!WARNING]
> Auto-generated names are **intentionally disposable**. They work for development and quick prototyping, but a new suffix appears on every restart, making it hard to correlate the same logical worker across restarts in dashboards and logs.

**Provided**

```
{workerName}-{timestampId}
```

Example: `payroll-01-3c1a8b2`

The timestamp suffix is always appended to ensure the runtime name is unique, even when the configured name is fixed.

> [!TIP]
> For production deployments always provide a stable, meaningful name such as `"Payroll-Worker-01"`. This keeps logs, dashboards, and alerting rules readable and consistent across restarts.

### Worker Lanes (Workload Isolation)
Lanes allow you to physically isolate different types of business logic. 
This ensures that a heavy, slow-running process (like "Report Generation") does not steal resources from a high-priority process (like "Transactional Emails").
Example: You can have one Worker dedicated to the `Default` lane and another Worker on a high-spec machine dedicated to the `Heavy-Compute` lane.

### Agent Worker Modes
The AgentWorkerMode defines the specific responsibility of a node within the JobMaster cluster. By decoupling the "Brain" (Coordination) from the "Muscle" (Execution), you can tailor your infrastructure to handle massive horizontal scale without overloading your databases
if none is defined, it will use the Full mode (default)

#### Full
The All-in-One Solution. This is the default mode. It handles the entire job lifecycle:
- Scans the Master Database.
- Onboards jobs to Agent Buckets.
- Executes handlers.

Guidance: If you are unsure which mode to use, start with Full. It performs all cluster roles simultaneously.

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
| Feature                                  | Full | Coordinator | Execution | Drain |
|:-----------------------------------------| :---: | :---: | :---: |:-----:|
| **Scan Master DB** (Look-ahead)          | ✅ | ✅ | ❌ |   ❌   |
| **Onboard to Buckets** (Transient)       | ✅ | ✅ | ❌ |   ❌   |
| **Claim Buckets** (Atomic Lock)          | ✅ | ❌ | ✅ |    ❌    |
| **Execute Handlers** (Compute)           | ✅ | ❌ | ✅ |   ❌   |
| **Sync State to Master** (Audit)         | ✅ | ❌ | ✅ |   ✅   |
| **Accept New Work** (Onboarding)         | ✅ | ✅ | ✅ |   ❌   |
| **Recovery of Orphaned Buckets (Drain)** | ✅ | ✅ |   ❌   | ✅ 

---

### The Parallelism Factor & Backpressure
JobMaster manages its own internal `TaskQueueControl` for each bucket to prevent CPU and Memory exhaustion. You don't need to manually calculate thread counts; you simply set a Factor.

### The Parallelism Factor & Backpressure

JobMaster handles execution scaling through an intelligent "Factor" system. Instead of manually managing thread pools, you define a multiplier that works in harmony with job priorities.

#### 1. Run Capacity Logic
The system calculates a base execution capacity determined by the **Job Priority** and scales it by your configured `ParallelismFactor`. This ensures that critical work always receives a higher share of resources than background tasks.

$$\text{RunCapacity} = \text{BasePriorityCapacity} \times \text{Factor}$$
| Priority | Base Capacity | Factor (1.0) | Factor (2.0) |
| :--- | :---: | :---: | :---: |
| **Very Low** | 2 | 2 | 4 |
| **Low** | 3 | 3 | 6 |
| **Medium** | 4 | 4 | 8 |
| **High** | 5 | 5 | 10 |
| **Critical** | 6 | 6 | 12 |

---

#### 2. Self-Governing Backpressure
To prevent memory exhaustion and "choking" under heavy load, every worker implements a strictly enforced backpressure mechanism via the `TaskQueueControl`.

* **Queue Buffer:** The worker maintains a local waiting queue with a capacity of:
  $$\text{WaitingCapacity} = \text{RunCapacity} \times 5$$
* **Flow Control:** If the internal waiting queue reaches its limit, the Worker **automatically pauses** fetching new jobs from the Agent Bucket.
* **Stability:** This ensures that the application remains responsive and stable even under 100% CPU load, as the system will not "over-ingest" more data than the hardware can currently process.

---

#### 3. Configuration Example
In your code, you only need to provide the factor. The system handles the math and the queue management internally:

```csharp
config.AddWorker()
    .ParallelismFactor(2.0); // Scales all priorities as shown in the table above
```

### Priority & Bucket Quantity
With `.BucketQtyConfig()`, you define how many buckets this worker should own for a specific priority.

- Higher Quantity: Increases the potential for parallel processing across the cluster.
- Lower Quantity: Reduces database connection overhead and resource footprint.

---

## Throughput & Batch Optimization

### 1. TransferBatchSize — DB Fetch Efficiency

Controls how many jobs are pulled per database round-trip when the Coordinator moves jobs from the Master DB into Agent Buckets, and in other bulk operations (e.g. drain runners, recurring schedule scans).

* **Reduced IOPS:** A value of `1000` means a single query claims up to 1,000 jobs at once, dramatically reducing database round-trip overhead at high volume.
* **Memory impact is low:** These records are transferred and released quickly; they do not stay resident in memory for their entire execution lifetime.
* **Default:** `1000` (standalone mode: `250`)

```csharp
.TransferBatchSize(1000)
```

| Volume | Recommended | Reasoning |
| :--- | :---: | :--- |
| Low (< 10k jobs/day) | `100 – 250` | Smaller footprint, faster round-trips |
| Medium | `500 – 1000` | Good balance of throughput and memory |
| High (millions/day) | `1000 – 5000` | Minimise DB round-trips under sustained load |

---

### 2. BucketBufferSize — In-Memory Execution Buffer

Controls the maximum number of jobs a bucket holds in memory at any one time while waiting to be executed. This is a per-bucket limit — each bucket owned by the worker maintains its own independent buffer.

* **Flow control:** When the buffer is full, incoming deliveries are bounced back to the Master (`HeldOnMaster`) with a short delay and retried. This prevents unbounded memory growth under load spikes.
* **Sizing guidance:** Match this to the throughput your worker can actually sustain within the `BucketBufferLeadTime` window. Oversizing wastes memory; undersizing causes unnecessary bouncing.
* **Default:** `250`

```csharp
.BucketBufferSize(250)
```

| Job Complexity | Recommended | Reasoning |
| :--- | :---: | :--- |
| **Micro-Jobs** (< 1s) | `500 – 1000` | High throughput; buffer empties quickly |
| **Standard** (1s – 5s) | `100 – 500` | Balanced; buffer is held for a few seconds |
| **Heavy-Jobs** (> 30s) | `10 – 50` | Avoids locking many slow jobs to one worker |

---

### 3. BucketBufferLeadTime — Look-Ahead Window

Controls how far ahead in time the worker pre-loads jobs into the in-memory buffer. Only jobs scheduled to run within this window are pulled from the Agent into memory.

* **Range:** between `250ms` and `30s` (enforced at startup).
* **Too short:** The buffer may run dry between fills, leaving execution slots idle.
* **Too long:** Jobs sit in memory longer than necessary. Under heavy load, if a job cannot be executed within its deadline window it is redirected back to the Master as `HeldOnMaster` and rescheduled — adding latency before it runs.
* **Default:** `30s`

```csharp
.BucketBufferLeadTime(TimeSpan.FromSeconds(30))
```

> [!TIP]
> **Tuning together:** `BucketBufferSize` and `BucketBufferLeadTime` work as a pair. If workers are sitting idle while jobs remain in the database, increase `BucketBufferSize` or widen `BucketBufferLeadTime`. If you see high memory usage or frequent bounce-backs (`HeldOnMaster`), reduce `BucketBufferSize`. If you need to reduce DB load from bucket transfers, increase `TransferBatchSize`.

See: [AgentsConfiguration](AgentsConfiguration.md)