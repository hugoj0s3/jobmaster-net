# DataRetentionArchiveTest

The real, unmodified archive-then-delete flow: once `DataRetentionTtl` passes, `DeleteOldFinalJobsRunner` and
`DeleteOldInactiveRecurringSchedulesRunner` copy finished jobs and terminated recurring schedules to the
`TargetArchivedClusterId` cluster (same ids, same final status, executions and failure logs too), then delete them
from the source. Replaces phase 1 of the earlier per-provider suite's `ArchivedModeTest` (5 per-provider
copies there); its phases 2–3 (the archived-mode safety net) are the matrix `ArchivedModeTest`. Single phase.

## Topology
One container `mx-<flavor>-archive` hosts both clusters (the purge runners forward to the archive cluster through
`JobMasterClusterAwareComponentFactories.TryGetFactory`, which only resolves clusters in the same process):
- `mx-<flavor>-arch-source`: an ordinary Active cluster, `DataRetentionTtl` 10 min (`JobMasterLimits.MinDataRetentionTtl`),
  `TargetArchivedClusterId` the target, one agent connection (`agent-1`), a coordinator and an execution worker.
  `TransientThreshold` 2 min (NATS caps it at 5).
- `mx-<flavor>-arch-target`: `Mode: Archived`, coordinator only (archived clusters only run a coordinator).

## Flow
1. Schedule 199 `fast` jobs and 1 that fails its only attempt (`injectFailure`, no retries), and a `00:06:00`
   recurring schedule. Wait until the schedule is on master (it goes through the agent first), then cancel it.
2. The 199 run exactly once and are `Succeeded`, the other is `Failed` (10 min max). On a timeout, a diagnostic file
   with the missing jobs, buckets, workers and container logs is written and named in the failure.
3. After the TTL (the purge runners poll every `Clamp(TTL/2, 5 min, 1 h)` = 5 min; 20 min max): the source has none
   of the 200 left; the target has all 200 with their ids and final status, a sample of executions, the failed
   job's `Failed` execution and its JobExecution log (and the source no longer has that log).
4. The cancelled schedule leaves the source and is on the target with its id, `Canceled`.

## Flavors
All six (the source's agent connection is the flavor's).
