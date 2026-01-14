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
        .WorkerBatchSize(1000)              // Jobs to fetch per DB round-trip and also onboarding list of a bucket (30 seconds to be processed)
        .ParallelismFactor(2)               // Scaler for concurrent execution
        .SetWorkerMode(AgentWorkerMode.Standalone)
        .BucketQtyConfig(JobMasterPriority.Critical, 3);
});
```

### Worker Lanes (Workload Isolation)
Lanes allow you to physically isolate different types of business logic. 
This ensures that a heavy, slow-running process (like "Report Generation") does not steal resources from a high-priority process (like "Transactional Emails").
Example: You can have one Worker dedicated to the `Default` lane and another Worker on a high-spec machine dedicated to the `Heavy-Compute` lane.

### Agent Worker Modes
The AgentWorkerMode defines the specific responsibility of a node within the JobMaster cluster. By decoupling the "Brain" (Coordination) from the "Muscle" (Execution), you can tailor your infrastructure to handle massive horizontal scale without overloading your databases
if none is defined, it will use the Standalone mode (default)

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

## Throughput & Batch Optimization

The `.WorkerBatchSize()` setting is a critical performance "knob" that governs how the Worker interacts with the Agent storage and how the Coordinator handles job onboarding.

### 1. Database Efficiency
Instead of querying the database for every single job, the worker pulls jobs in large "chunks."

* **Reduced IOPS:** Setting this to `1000` means the worker performs **one** database round-trip to claim up to 1,000 jobs.
* **Impact:** This drastically reduces the overhead on both your Agent and Master databases, allowing a single connection pool to handle millions of jobs without becoming a bottleneck.
* **Memory Usage:** Note that a larger batch size increases the memory footprint of the worker, as it must hold the job metadata in its local buffer.


---

### 2. The 30-Second Processing Goal
A key design principle of JobMaster is maintaining a tight synchronization between the database state and the in-memory execution state.

* **Per-Bucket Onboarding:** It is important to remember that the batch size applies **per bucket**. If a worker owns multiple buckets, each bucket will maintain its own onboarding list based on this limit.
* **The Onboarding List:** This batch size determines the maximum number of jobs that stay in local memory for processing during an onboarding window (targeting a ~30-second completion cycle).

| Job Complexity | Recommended Batch | Reasoning |
| :--- | :--- | :--- |
| **Micro-Jobs** (< 1s) | `1000 - 5000` | High volume, low CPU. Maximizes throughput. |
| **Standard** (1s - 5s) | `100 - 500` | Balanced approach for typical API or data tasks. |
| **Heavy-Jobs** (> 30s) | `10 - 50` | Prevents too many heavy tasks from being "locked" to one worker. |

> [!TIP]
> **Performance Scaling:** If your workers are sitting idle while work remains in the database, increase the `WorkerBatchSize`. If your workers are hitting memory limits or taking too long to sync, decrease the batch size to reduce the local memory footprint.
