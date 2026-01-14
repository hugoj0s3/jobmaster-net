# Cluster Configuration

The Cluster configuration acts as the "Brain" of the operation. It defines the global rules, safety boundaries, and orchestration policies that all Workers and Agents must follow to ensure system-wide consistency.

## Cluster-level Settings

These global policies define the default behavior for the entire ecosystem. Most of these values act as a "safety net" and can be overridden at the specific Job or Schedule level.
---

#### `DefaultJobTimeout` (Default: 5 minutes)
The maximum time a single job is allowed to run before being forcefully terminated.
* **Resource Protection:** Prevents "runaway" or hung executions from consuming worker threads indefinitely.
* **Retry Integration:** Timed-out jobs are automatically marked for retry according to the cluster's retry policy.
* **Override:** Can be overridden at the class level using the `[JobMasterTimeout]` attribute.

#### `TransientThreshold` (Default: 10 minutes)
The "Look-ahead" window used by the Coordinator to identify near-future jobs for onboarding from the Master DB to Agent Buckets.
* **Range:** Minimum **10 seconds**, Maximum **24 hours**.
* **High Throughput:** Large values are better for stable environments, as they reduce the frequency of Master database scans and allow for more efficient.
* **Dynamic Scaling:** Small values are recommended for environments like Kubernetes, where worker nodes may scale down or restart frequently (minimizing "orphaned" work in transient storage).

#### `DefaultMaxOfRetryCount` (Default: 3)
The global default for how many times a failing job should be retried before being moved to the terminal `Failed` state.
* **Override:** Can be overridden per job using the `[JobMasterMaxNumberOfRetries]` attribute or dynamically during the `.Schedule()` call.

#### `ClusterMode` (Default: Active)
Governs the operational state and behavior of the entire cluster.
* **`Active`**: Standard operation mode. All scheduling and execution systems are enabled.
* **`Passive`**: Maintenance mode. Jobs can be scheduled and saved, but workers will not pull or execute work.
* **`Archive`**: Read-only mode. Used for historical audits; no new jobs can be scheduled or executed.

#### `MaxMessageByteSize` (Default: 128 KiB)
The upper bound for serialized job payloads used as a pre-dispatch safety check.
* **Guidance:** Set this slightly below your transport provider’s hard limit (e.g., if using NATS with a 1MB limit, set this to ~950 KiB to account for metadata overhead).

#### `IanaTimeZoneId` (Default: System Local)
The source of truth for interpreting recurring schedules (e.g., "Daily at 12:00 PM").
* **Consistency:** In distributed clusters spanning multiple regions, explicitly setting this (e.g., `America/Sao_Paulo`) ensures that a cron job runs at the same moment regardless of where the physical server is located.

#### `DataRetentionTtl` (Default: 30 days)
Controls the automatic lifecycle of job history and audit logs in the Master database.
* **Scope:** Automatically cleans up completed jobs, expired schedules, and execution logs.
* **Tuning:** Set to `null` to keep history indefinitely. In high-throughput environments, a lower TTL is vital to keep the Master database indices performing optimally and prevent excessive storage growth.

---

### Implementation Example

```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("My-Cluster")
        .UsePostgresForMaster("Host=localhost;Database=jobmaster;...")
        .ClusterMode(ClusterMode.Active)
        .ClusterTransientThreshold(TimeSpan.FromMinutes(20))
        .ClusterDefaultJobTimeout(TimeSpan.FromMinutes(1))
        .ClusterDefaultMaxRetryCount(3)
        .ClusterMaxMessageByteSize(256 * 1024)
        .ClusterIanaTimeZoneId("America/Sao_Paulo")
        .SetAsDefault();
});
