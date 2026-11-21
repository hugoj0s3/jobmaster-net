# MigratingModeTest

The real, unmodified `MigrateJobsRunner` and `MigrateRecurringSchedulesRunner` (30 s ticks) move everything from a
Migrating cluster to its target, and the target's normal pipeline finishes it: no job lost, none duplicated, same ids
end to end (migration only changes `ClusterId`, it never re-creates a job). Replaces
the earlier per-provider suite's `MigratingModeTest` (5 per-provider copies there). Single phase.

## Topology
One container `mx-<flavor>-migration` hosts both clusters (cross-cluster forwarding only resolves clusters
registered in the same process):
- `mx-<flavor>-mig-source`: `Mode: Migrating`, `TargetActiveClusterId` the target, coordinator only. A Migrating
  cluster never has buckets: every job scheduled there stays on master until migrated.
- `mx-<flavor>-mig-target`: an ordinary Active cluster, one agent connection (`agent-1`, the flavor's agent provider),
  a coordinator and an execution worker. `TransientThreshold` 2 min (NATS caps it at 5).

## Flow
1. Schedule 500 `fast` jobs and a `TimeSpanInterval` `00:00:15` recurring schedule on the source.
2. The source's job count reaches 0, and its recurring schedule list empties (5 min max each).
3. All 500 run exactly once on the target and are `Succeeded` there with their original ids (10 min max).
4. The schedule is on the target with the same id, `Active`, and fires there; then it's cancelled.

## Flavors
All six (the target's agent connection is the flavor's).
