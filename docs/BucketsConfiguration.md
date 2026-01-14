# Core Mechanics: Workers, Buckets, and Life Cycle
To understand JobMaster, you must first understand how work is partitioned and executed. 
The system is designed to maximize concurrency while completely eliminating database deadlocks.

## What is a Bucket?
A bucket is the fundamental logical unit of concurrency. 
Instead of every worker in your cluster competing for individual jobs (which causes massive database locking), workers create and own Buckets.

**Atomic Locking**: When a worker owns a bucket, it takes exclusive ownership of every job inside it for that cycle.

**Parallelism**: If you have 10 workers and 10 buckets, each worker takes one bucket, and they all work in perfect parallel without ever touching the same data.

**Configuration** (BucketQtyConfig): You define the number of buckets per priority.

**High Volume**: More buckets = Higher parallelism.

**Heavy Jobs**: Fewer buckets = Less resource strain per worker.

```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("My-Cluster");
    ...
        
    config.AddWorker()
        .AgentConnName("MyPostgresAgent-1")
        .BucketQtyConfig(JobMasterPriority.VeryLow, 1)
        .BucketQtyConfig(JobMasterPriority.Low, 2)
        .BucketQtyConfig(JobMasterPriority.Medium, 3)
        .BucketQtyConfig(JobMasterPriority.High, 4)
        .BucketQtyConfig(JobMasterPriority.Critical, 5);
});

```

## Job Life Cycle
Every job in JobMaster follows a strictly defined state machine. 
Monitoring these statuses allows you to observe exactly how work moves from your code to the database and eventually to execution.

| Status                 | Phase | Description                                                                                                                                                                           | Storage Location |
|:-----------------------| :--- |:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| :--- |
| **`SavePending`**      | **Entry Point** | **High-Throughput Mode.** The job is accepted by the Agent but not yet persisted to the Master DB. This allows the calling application to continue without waiting for Master DB I/O. | Agent (Transient) |
| **`HeldOnMaster`**     | **Durable Storage** | Jobs scheduled for the future. They stay here to keep the Agent lean, only moving to a bucket once they fall within the TransientThreshold window.                         | Master Database |
| **`AssignedToBucket`** | **Onboarding** | The job has been moved to an Agent Worker Bucket. It is now visible to execution nodes and ready to be claimed.                                                                       | Agent (Bucket) |
| **`Queued`**           | **Staging** | The job has been pulled from the bucket into a specific worker's local memory and is awaiting an available execution thread.                                                          | Worker Memory |
| **`Processing`**       | **Execution** | Your `IJobHandler` logic is currently running. The worker holds an active claim on this job.                                                                                          | Worker Thread |
| **`Succeeded`**        | **Terminal** | The handler finished successfully. The result is synced back to the Master DB for auditing.                                                                                           | Audit Log (Master) |
| **`Failed`**           | **Terminal** | The job has exhausted all retry attempts without success.                                                                                                                             | Audit Log (Master) |
| **`Cancelled`**        | **Terminal** | The job was manually or programmatically aborted before completion.                                                                                                                   | Audit Log (Master) |

## Bucket Life Cycle
Bucket Life Cycle & Self-Healing
In JobMaster, Buckets are not static; they have their own life cycle governed by the health of the Agent Workers. 
This state machine ensures that even if a server crashes, no job is ever lost in the system.

### Bucket Status Definitions

| Status | Phase | Description                                                                                                                                                        |
| :--- | :--- |:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **`Active`** | **Operation** | The normal operating state. The bucket is owned by a healthy **Agent Worker** and is actively onboarding and processing new jobs.                                  |
| **`Completing`** | **Graceful Exit** | **Proactive Shutdown.** The bucket stops onboarding new jobs but continues to execute current work and syncs all `SavePending` items.                              |
| **`Lost`** | **Hard Crash** | **Reactive Recovery.** An **Agent Worker** stopped heartbeating unexpectedly. The bucket is orphaned, and its contents must be rescued.                            |
| **`Draining`** | **Recovery** | A healthy **Agent Worker** has claimed a `Lost` bucket. It is physically moving jobs back to the Master DB (`HeldOnMaster`) for redistribution across the cluster. |
| **`ReadyToDelete`** | **Finalization** | The bucket is empty and all states are synced. It is now a "tombstone" awaiting permanent deletion from the Agent storage.                                         |

---

### The "Graceful Exit" Flow (`Completing`)

When an **Agent Worker** is signaled to shut down (e.g., during a deployment), it moves its buckets into the `Completing` state. This is a critical feature for **Zero-Downtime Architecture**:

1.  **Block Intake:** The bucket immediately stops pulling new jobs from the Master Database.
2.  **Finish Active Work:** Jobs currently in `Processing` or `Queued` are allowed to complete naturally.
3.  **Flush Persistence:** Every job marked as `SavePending` (not yet in the Master DB) is prioritized for a final sync to ensure no data is lost.
4.  **Clean Retirement:** Once the bucket is empty, the **Agent Worker** shuts down safely.



---

### The "Orphan" Rescue Flow (`Lost` → `Draining`)

If an **Agent Worker** crashes or loses network connectivity, JobMaster heals the cluster automatically:

1.  **Detection:** The system identifies a missing heartbeat and marks the affected buckets as **`Lost`**.
2.  **Takeover:** A healthy **Agent Worker** claims the orphaned bucket.
3.  **Redistribution:** The healthy worker enters **`Draining`** mode, moving all unfinished jobs back to the Master Database.
4.  **Re-Onboarding:** These jobs return to the **`HeldOnMaster`** status, where they will be naturally picked up by active buckets on other healthy workers.

See: [WorkersConfiguration](WorkersConfiguration.md)