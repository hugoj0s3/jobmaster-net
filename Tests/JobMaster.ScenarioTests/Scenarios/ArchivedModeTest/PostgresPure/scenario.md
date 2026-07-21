# ArchivedModeTest.PostgresPure

Tests `ClusterMode.Archived` end to end: a finalized job on an Active cluster with `DataRetentionTtl`
+ `TargetArchivedClusterId` set gets archived (copied, status-preserved) into the Archived cluster
before being purged from the source.

## Topology

**One container, two clusters** — same reasoning as `MigratingModeTest`: `DeleteOldFinalJobsRunner`
resolves the archive target via `JobMasterClusterAwareComponentFactories.TryGetFactory`, which only
finds clusters registered in the same process, so both live in one container's
`clusterConfigTemplates` array.

| Cluster | Mode | DataRetentionTtl | Workers | Notes |
|---|---|---|---|---|
| `archive-source` | `Active` (default) | `00:10:00` | Coordinator + 1 Execution worker | An entirely ordinary Active cluster otherwise — jobs onboard and execute normally. `00:10:00` is `JobMasterDefaults.MinDataRetentionTtl`, the framework's floor (the fastest this can be configured). |
| `archive-target` | `Archived` | — | Coordinator only | No agent connections — `PreValidation` forbids them on an Archived cluster ("Archive clusters only run Coordinator"). |

`archive-source.TargetArchivedClusterId = "archive-target"`.

**The recurring schedule uses `TestAppFastHandler` (handlerType `"fast"`), not a dedicated recurring
handler** — same reasoning as `MigratingModeTest`: the planning process (this container's Coordinator)
must have the handler type loaded, and `TargetTestScheduleApp`'s own handlers qualify while
`TargetTestRecurringApp`'s don't. This scenario cancels the schedule immediately after creating it, so
`RecurringSchedulePlanner` never actually plans an occurrence either way (a non-`Active` schedule
returns before handler-type resolution) — but the same `/recurring-schedule/{handlerType}` endpoint on
`TargetTestScheduleApp` is used for consistency with `MigratingModeTest`, avoiding a second container.

## Flow

1. Schedule 200 `fast` jobs against `archive-source`; wait for all 200 to reach `Succeeded` the
   normal way (Redis-tracked executions, fast).
2. Wait for `archive-source`'s job count (filtered by this run's `TestIdentifier`) to reach 0.
   `DeleteOldFinalJobsRunner`'s own poll interval is derived from the TTL
   (`Clamp(TTL/2, 5min, 1hr)` => 5 minutes at a 10-minute TTL), so this realistically takes the 10-minute
   TTL itself plus up to one more 5-minute tick to notice — budgeted 20 minutes, same calibration
   `DataRetentionPhase2EmulatorBase` already uses for plain (non-archiving) TTL purge.
3. Assert `archive-target` now holds exactly those 200 jobs: same job IDs (the intake service's
   `ReassignToArchiveCluster` only changes `ClusterId`, never re-creates the job) and still
   `Succeeded` (the status they had at purge time, not reset).
4. In parallel with the jobs above: create one recurring schedule (`TimeSpanInterval`, 6min) against
   `archive-source`, then immediately cancel it — `DeleteOldInactiveRecurringSchedulesRunner` is
   gated by the exact same `DataRetentionTtl`/`TargetArchivedClusterId` config already set for the
   jobs, and only considers *terminated* schedules, so cancelling is what makes it a candidate. Wait
   for it to leave `archive-source` (same TTL/poll timing as the jobs), then assert it exists on
   `archive-target` with the same Id and still `Canceled` (the status it had at purge time).

## Multi-variant note

`TestIdentifier`/`RecurringTestIdentifier` are generated fresh (`Guid.NewGuid()`) per run rather than
literal constants — every repo-type variant shares one Redis instance
(`ScenarioGlobalEnvironment`), and `[Collection(ScenarioCollection.Name)]` runs them sequentially
without clearing Redis in between, so a literal identifier would let a later variant's
`Tracker.WaitForAsync` match an earlier variant's already-recorded executions.

## What this doesn't cover (deliberately)

- **Archive target unreachable** — `DeleteOldFinalJobsRunner`/`DeleteOldInactiveRecurringSchedulesRunner`
  log `Critical` and fall back to a plain (non-archiving) delete if the target cluster's factory
  can't be resolved — a real "you are losing data" signal by design. Not exercised here.

## Timing

Both legs — reaching the 10-minute TTL, and `DeleteOldFinalJobsRunner`'s own 5-minute poll noticing
it — are real, unmodified JobMaster timing, not shortened for this test (same floor
`PostgresPure`'s Phase2 DataRetentionTtl work already established). Expect this scenario to take
roughly 12-18 minutes end to end; `ArchiveWaitTimeout` (20 min) has headroom above that.
