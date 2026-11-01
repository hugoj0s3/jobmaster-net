# Background runners: where each one runs

Which runners a worker starts depends on two things: the **cluster mode** (Active, Migrating, Archived) and the
**worker mode** (Coordinator, Execution, Drain, Full). The source of truth is
[`JobMasterBackgroundAgentWorker.StartAsync`](../JobMasterBackgroundAgentWorker.cs) and the `Load*RunnersAsync`
methods it calls.

## Allowed worker modes per cluster mode

| Cluster mode | Coordinator | Execution | Drain | Full |
|---|---|---|---|---|
| Active | yes | yes | yes | yes |
| Migrating | yes | **no** | yes | yes (no execution runners; acts as Coordinator + Drain) |
| Archived | yes | **no** | **no** | yes (behaves exactly like Coordinator, no agent connection) |

Enforced at startup by `ValidateClusterDefinitions` in [`JobMasterRuntime`](../JobMasterRuntime.cs).

## Matrix

Legend: **✓** runs · **no-op** started, but returns right away on that cluster mode · **—** not started.
A value in brackets is the interval when it differs from the runner's default.

| Runner | Active Coord | Active Exec | Active Drain | Active Full | Migrating Coord | Migrating Drain | Migrating Full | Archived Coord / Full |
|---|---|---|---|---|---|---|---|---|
| **Heartbeats** | | | | | | | | |
| [`KeepAliveWorkerRunner`](KeepAliveWorkerRunner.cs) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| [`KeepAliveHostRunner`](KeepAliveHostRunner.cs) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| [`KeepAliveAgentConnectionRunner`](KeepAliveAgentConnectionRunner.cs) | — | ✓ | ✓ | ✓ | — | ✓ | ✓ | — |
| **Execution (one set per bucket)** | | | | | | | | |
| [`PollingSavePendingJobsRunner`](SavePendingJobs/PollingSavePendingJobsRunner.cs) | — | ✓ | — | ✓ | — | — | — | — |
| [`PollingJobsExecutionRunner`](JobsExecution/PollingJobsExecutionRunner.cs) | — | ✓ | — | ✓ | — | — | — | — |
| [`PollingSaveRecurringScheduleRunner`](SavePendingRecurringSchedule/PollingSaveRecurringScheduleRunner.cs) | — | ✓ | — | ✓ | — | — | — | — |
| [`RecentlyInsertedScheduleRecurringJobsRunner`](JobAndRecurringScheduleLifeCycleControl/RecentlyInsertedScheduleRecurringJobsRunner.cs) | — | ✓ | ✓ | ✓ | — | ✓ | ✓ | — |
| **Dispatch and recurring planning** | | | | | | | | |
| [`AssignJobsToBucketsRunner`](JobAndRecurringScheduleLifeCycleControl/AssignJobsToBucketsRunner.cs) | ✓ | — | — | ✓ | no-op | no-op | no-op | — |
| [`ScheduleRecurringJobsRunner`](JobAndRecurringScheduleLifeCycleControl/ScheduleRecurringJobsRunner.cs) | ✓ | — | — | ✓ | no-op | no-op | no-op | — |
| [`StaticRecurringDefinitionsKeepAliveRunner`](JobAndRecurringScheduleLifeCycleControl/StaticRecurringDefinitionsKeepAliveRunner.cs) | ✓ | — | — | ✓ | ✓ | ✓ | ✓ | — |
| [`CancelJobsFromRecurScheduleInactiveOrCanceledRunner`](JobAndRecurringScheduleLifeCycleControl/CancelJobsFromRecurScheduleInactiveOrCanceledRunner.cs) | ✓ | — | — | ✓ | ✓ | ✓ | ✓ | — |
| **Job recovery** | | | | | | | | |
| [`HeldOnMasterDeadlineTimeoutJobsRunner`](JobAndRecurringScheduleLifeCycleControl/HeldOnMasterDeadlineTimeoutJobsRunner.cs) | ✓ | — | — | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Buckets** | | | | | | | | |
| [`MarkBucketAsLostRunner`](BucketLifeCycleControl/MarkBucketAsLostRunner.cs) | ✓ | — | ✓ (30 s) | ✓ | ✓ | ✓ | ✓ | ✓ |
| [`AssignedLostBucketsRunner`](BucketLifeCycleControl/AssignedLostBucketsRunner.cs) | ✓ | — | ✓ (10 s) | ✓ ×2 | ✓ | ✓ ×2 (1 min + 10 s) | ✓ ×2 | — |
| [`DestroyReadyToDeleteBucketsRunner`](BucketLifeCycleControl/DestroyReadyToDeleteBucketsRunner.cs) | ✓ | — | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| [`DrainRunnersCoordinator`](BucketLifeCycleControl/DrainRunnersCoordinator.cs) | — | — | ✓ (10 s) | ✓ (1 min) | — | ✓ (10 s) | ✓ (1 min) | — |
| **Migration** | | | | | | | | |
| [`MigrateJobsRunner`](Migration/MigrateJobsRunner.cs) | — | — | — | — | ✓ (30 s) | ✓ (30 s) | ✓ (30 s) | ✓ (2.5 min) |
| [`MigrateRecurringSchedulesRunner`](Migration/MigrateRecurringSchedulesRunner.cs) | — | — | — | — | ✓ (30 s) | ✓ (30 s) | ✓ (30 s) | ✓ (2.5 min) |
| **Worker lifecycle and cleanup** | | | | | | | | |
| [`WorkerStopCoordinatorRunner`](WorkerStopCoordinatorRunner.cs) | ✓ | — | — | ✓ | ✓ | ✓ | ✓ | — |
| [`CleanupDeadWorkerRunner`](CleanupDeadWorkerRunner.cs) | ✓ | — | — | ✓ | ✓ | ✓ | ✓ | — |
| [`CleanupDeadAgentConnectionsRunner`](CleanupDeadAgentConnectionsRunner.cs) | ✓ | — | — | ✓ | ✓ | ✓ | ✓ | — |
| [`CleanupDeadHostsRunner`](CleanupDeadHostsRunner.cs) | ✓ | — | — | ✓ | ✓ | ✓ | ✓ | — |
| **Data retention** | | | | | | | | |
| [`DeleteExpiredGenericRecordsRunner`](CleanUpData/DeleteExpiredGenericRecordsRunner.cs) | ✓ | — | — | ✓ | ✓ | ✓ | ✓ | ✓ |
| [`DeleteOldFinalJobsRunner`](CleanUpData/DeleteOldFinalJobsRunner.cs) | ✓ | — | — | ✓ | ✓ | ✓ | ✓ | ✓ |
| [`DeleteOldInactiveRecurringSchedulesRunner`](CleanUpData/DeleteOldInactiveRecurringSchedulesRunner.cs) | ✓ | — | — | ✓ | ✓ | ✓ | ✓ | ✓ |
| [`DeleteOldLogsRunner`](CleanUpData/DeleteOldLogsRunner.cs) | ✓ | — | — | ✓ | ✓ | ✓ | ✓ | ✓ |

[`DrainRunnersCoordinator`](BucketLifeCycleControl/DrainRunnersCoordinator.cs) starts one set per bucket being drained
by this worker:
[`PollingDrainSavePendingJobsRunner`](DrainRunners/PollingDrainSavePendingJobsRunner.cs),
[`PollingDrainProcessingJobsRunner`](DrainRunners/PollingDrainProcessingJobsRunner.cs),
[`PollingDrainSavePendingRecurringScheduleRunner`](DrainRunners/PollingDrainSavePendingRecurringScheduleRunner.cs) and
[`MarkBucketReadyToDeleteRunner`](DrainRunners/MarkBucketReadyToDeleteRunner.cs).

## Notes

- **Migrating loads the whole coordinator set for every worker mode**, Drain included. Dispatch and recurring
  planning are no-ops there because those runners only work on Active clusters.
- **Archived uses its own short list**: heartbeats, data retention, the migrate runners, and the two runners that
  bring jobs of dead workers back to master (`MarkBucketAsLostRunner`, `HeldOnMasterDeadlineTimeoutJobsRunner`).
  `AssignedLostBucketsRunner` is left out on purpose: there's no Drain or Full worker to hand a Lost bucket to.
- **Standalone clusters** are Active clusters; [`JobMasterRuntime`](../JobMasterRuntime.cs) adds a
  `StandaloneDrainer` Drain worker next to the configured ones.
- **`AssignedLostBucketsRunner` is started twice** on Active Full and on Migrating Drain/Full: once by the coordinator
  set and once by the drain path. Both instances take the same distributed lock, so it's harmless, only redundant.
