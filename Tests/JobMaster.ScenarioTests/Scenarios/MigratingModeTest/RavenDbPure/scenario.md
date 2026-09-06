# MigratingModeTest.RavenDbPure

Tests `ClusterMode.Migrating` end to end: a job scheduled on a Migrating cluster is forwarded by
`MigrateJobsRunner` to its `TargetActiveClusterId` cluster, and finishes there through that cluster's
entirely ordinary pipeline. Single phase — no crash/recovery scenario here, just proving the real,
unmodified background runner does what it's supposed to under real timing. Same test as
`PostgresPure`, `MySqlPure`, and `SqlServerPure`, run against the RavenDB provider instead of a SQL
one.

## Topology

**One container, two clusters** — deliberate, not incidental. `MigrateJobsRunner` resolves its target
via `JobMasterClusterAwareComponentFactories.TryGetFactory(cfg.TargetActiveClusterId)`, which only
ever finds clusters registered in the *same process*. So both clusters have to live in one app
container's `clusterConfigTemplates` array (already a JSON array per container — `Program.cs` already
loops over it calling `AddJobMasterCluster` once per entry, no app code changes needed):

| Cluster | Mode | Workers | Notes |
|---|---|---|---|
| `migrating-source` | `Migrating` | Coordinator only | No agent connections at all — `PreValidation` forbids buckets on a Migrating cluster, and a Coordinator never needs one anyway. Every job scheduled here is held `OnMaster` forever (`AssignJobsToBucketsRunner`, the only thing that would onboard a job into a bucket, only runs on `Active` clusters) until `MigrateJobsRunner` moves it. |
| `migrating-target` | `Active` (default) | Coordinator + 1 Execution worker | Nothing special — a minimal, ordinary single-worker cluster, backed by a RavenDB agent connection (`raven-agent-target`, database `MigratingTargetAgent`). Once a job lands here it's indistinguishable from one scheduled directly against it. |

`migrating-source.TargetActiveClusterId = "migrating-target"`.

**The recurring schedule uses `TestAppFastHandler` (handlerType `"fast"`), not a dedicated recurring
handler.** `RecurringSchedulePlanner.ScheduleNextJobsAsync` resolves the schedule's job handler type
via `JobMasterDefinitionIdAttribute.GetJobHandlerTypeFromId(...)` — a reflection lookup against types
loaded in *whichever process is doing the planning* (the Coordinator worker for the cluster owning the
schedule at that moment). Both `migrating-source`'s and `migrating-target`'s Coordinators run inside
this same `raven-migration` container (`TargetTestScheduleApp`), so the handler type must be one that
process actually has loaded — `TargetTestScheduleApp`'s own `fast`/`normal`/`slow`/`verylong` handlers
qualify; `TargetTestRecurringApp`'s `RecurringTickHandler` does not, since that's a separate process
this container never loads. Using a handler type the planning process can't resolve doesn't error
loudly — `ScheduleNextJobsAsync` logs `Critical` and just delays `LastPlanCoverageUntil`, so the
schedule silently never fires. `TargetTestScheduleApp` was extended with `/recurring-schedule/{handlerType}`
(POST) and `/recurring-schedule/{id}` (DELETE) endpoints, mirroring `TargetTestRecurringApp`'s shape
but dispatching to its own already-loaded handlers, so this scenario needs no second container.

No `ConnectionOptions` tuning (e.g. `pooledConnectionLifetimeMs`) is set on either cluster's master
connection or the target's `raven-agent-target` agent connection — plain connection strings,
matching every other RavenDB scenario variant in this suite. That binder behavior is already covered
by unit tests (`Tests/JobMaster.UnitTests/Ioc/RavenDbConnectionOptionsTests.cs`).

## Flow

All real `RunAsync()` logic lives in `MigratingModeTestPhase1EmulatorBase` (one directory up) —
this variant just supplies `SourceClusterId`/`TargetClusterId`/`ContainerName`.

1. Schedule 500 `fast` jobs against `migrating-source`.
2. Wait for `migrating-source`'s total job count to reach 0 — since nothing else ever advances a job
   past `OnMaster` there, this *is* the "migration complete" signal, no status filter needed.
   `MigrateJobsRunner` ticks every 30s and moves up to `TransferBatchSize` (1000) jobs per tick, so
   500 jobs migrate in a single tick once visible — budgeted 5 minutes for scheduling-visibility
   delay plus a couple of ticks of headroom.
3. Wait for all 500 to reach `Succeeded` on `migrating-target` (Redis-tracked executions +
   API-persisted `Succeeded` jobs, cross-checked exactly — no loss, no duplicate execution), same as
   every other scenario in this suite.
4. Assert the exact set of job IDs is preserved end to end: `MigrateJobsRunner` calls
   `job.ReassignToCluster(...)`, which only ever changes `ClusterId` (protected setter, same pattern
   as the archive flow's `ReassignToArchiveCluster`) — the job's `Id` never changes, so the IDs
   `ScheduleAsync` returned up front must match exactly what's `Succeeded` on the target.
5. In parallel with the jobs above: create one recurring schedule (`TimeSpanInterval`, 15s) against
   `migrating-source`. Recurring schedules have no "held" sub-status the way jobs do — they're
   persisted `Active` immediately even under Migrating mode (`ScheduleRecurringJobsRunner`, the only
   thing that would plan their occurrences, only runs on `Active` clusters) — so it's a migration
   candidate from the moment it's created, same 30s `MigrateRecurringSchedulesRunner` tick. Wait for
   it to leave `migrating-source`, then assert it exists on `migrating-target` with the same Id and
   `Active` status, and that it's genuinely live there (not just a relocated row) by waiting for at
   least one occurrence to actually fire on the target's own 15s interval.

## What this doesn't cover (deliberately)

- **Target cluster unreachable** — `MigrateJobsRunner`/`MigrateRecurringSchedulesRunner` log and skip
  (leave the row where it is, nothing lost) if `TryGetFactory` can't resolve the target. Not
  exercised here; would need a phase where the target cluster's container isn't running yet, then
  comes online.
