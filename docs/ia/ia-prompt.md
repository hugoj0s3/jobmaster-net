# Core Concepts

## Core Idea
Orchestrate distributed background processing reliably. The agents data plane holds ephemeral state for in‑flight jobs; the master DB is the permanent source for orchestration and history. Job pre‑assignment is governed by TransientThreshold defined at the master level.

[planes]
Master DB (cold control plane): Stores clusters, topology, and policies; minimized hot-path I/O.
Agents (hot data plane): Redis/Kafka/etc. for queues/streams and in‑flight state.

## Master DB (Cold Control Plane)
- Role: Orchestrates clusters/topology/policies with low churn.
- Stores: clusters, agent connections, workers, policies, summaries.
- Code: `Sql/JobMaster.SqlBase/SqlJobMasterRuntimeSetup.cs`, `JobMaster.Core/Ioc/Setup/ClusterConfigBuilder.cs`, `JobMaster.Core/Config/JobMasterClusterConnectionConfig.cs`.

## Clusters
- Definition: Boundary grouping Agents, Workers, Buckets.
- Isolation: Per-cluster DI instances and keyspace.
- Heterogeneous Agents supported (e.g., Redis, Kafka) via adapters.

## Agent Connections (Hot Data Plane)
- Role: High‑churn queuing, in‑flight state via specific backends (Redis/Kafka/etc.).
- Code: `JobMaster.Core/Models/MasterConfigurationModel.cs`, `JobMaster.Core/Services/Agents/AgentJobsDispatcherService.cs`.

## Agent Workers
- Role: Own buckets, pre-assign jobs, execute with priority + timeouts, drain/recover, graceful stop.
- Code: `JobMaster.Core/Background/JobMasterBackgroundAgentWorker.cs`, `JobMaster.Core/Background/JobMasterRuntime.cs`, `JobMaster.Core/Services/Master/MasterAgentWorkersService.cs`.

## Buckets
- Concept: Primary concurrency/ownership unit per worker, avoiding cross-worker contention.
- Status: `Assigned`, `Lost`, `Draining`, `Completing`.
- Code: `JobMaster.Core/Models/Buckets/BucketModel.cs` (uses `AgentConnectionId`), `JobMaster.Core/Models/Buckets/BucketStatus.cs`.

## Held on Master (reassignment)
- State: `JobMasterJobStatus.HeldOnMaster`.
- Action: `JobRawModel.MarkAsHeldOnMaster()` (preferred; `HeldOnMaster()` is obsolete).
- Used by: Drain, deadline recovery, scheduling fallback, execution fallback.
- Code: `JobMaster.Core/Models/Jobs/JobRawModel.cs`.

## Execution and Timeouts (summary)
- Runner: `JobsExecutionRunner` with priority-based resources and timeout abortion (`TaskContainer.AbortTimeoutTasks`).
- Code: `JobMaster.Core/Background/Runners/JobsExecution/JobsExecutionRunner.cs`.


# Bucket Lifecycle

## States
- Assigned: Owned by a worker, accepts new jobs (`BucketStatus.Assigned`).
- Lost: Worker died; bucket orphaned (`BucketModel.MarkAsLost()`).
- Draining: Adopted to empty; do not execute jobs (`BucketModel.AssignLostBucket(...)`).
- Completing: Stop accepting jobs; finish in-flight, then release/delete (`BucketModel.MarkAsCompleting()`).

## Drain behavior (lost bucket)
- Another worker adopts bucket → Draining.
- For each job: `JobRawModel.MarkAsHeldOnMaster()`; persist changes. once it held on master it will be re-assigned to another bucket.
- When empty: safe to delete bucket.

## Graceful stop
- Buckets move to Completing, stop taking new jobs, finish in-flight, then release/delete.

## References
- `JobMaster.Core/Models/Buckets/BucketModel.cs`
- `JobMaster.Core/Models/Buckets/BucketStatus.cs`
- `JobMaster.Core/Models/Jobs/JobRawModel.cs`
- Runners: `DrainJobsRunner`, `ProcessDeadlineTimeoutRunner`, `JobsExecutionRunner`