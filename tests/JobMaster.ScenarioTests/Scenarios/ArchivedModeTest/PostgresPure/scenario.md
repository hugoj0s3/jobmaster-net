# ArchivedModeTest.PostgresPure

Tests `ClusterMode.Archived` end to end:
- **Phase 1:** a finalized job on an Active cluster with `DataRetentionTtl` + `TargetArchivedClusterId` set gets
  archived (copied, status-preserved) into the Archived cluster before being purged from the source.
- **Phases 2–3 (archived safety net, Plan0.0.12Alpha item 18):** jobs and a recurring schedule that land on a cluster
  right after it became Archived are moved to its `TargetActiveClusterId` and run there. PostgresPure only; the
  other variants run phase 1 only.

## Phase 1 topology

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

## Phase 1 flow

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

One of the 200 jobs uses `InjectFailure` with `maxNumberOfRetries: 0`, so a `Failed` job, its `Failed`
execution and its `JobExecution` log are archived too, not only the happy path.

**Timeout diagnostics.** If the execution wait times out, `DumpMissingJobsAsync` writes
`%TEMP%/jm-scenario-diag/<TestIdentifier>.txt` and the failure message names it. The file holds each missing job
(API view, executions, logs), the buckets and workers, the container logs, and (PostgresPure, `DumpRawAsync`)
the master rows referencing the missing ids plus every agent-database row. Found the one-job-short flake of
2026-10-31: the test app's lazy Redis connect timed out under the first job burst, the job failed once, and its
first retry (30 s + the 10-minute processing deadline) came after the 10-minute wait. Fixed in the test apps
(Redis connected at startup, `/health` 503 until connected).

## Phases 2–3: late work on an archived cluster

The scheduler's Archived check isn't atomic with its write, so a job or schedule can land on a cluster right after
it became Archived. With a target, the archived cluster's Coordinator moves them there
(`MigrateJobsRunner` / `MigrateRecurringSchedulesRunner`, every 2.5 min on Archived).

| Cluster | Phase 2 | Phase 3 | Database |
|---|---|---|---|
| `stray-old` | `Migrating`, `TargetActiveClusterId = stray-new`, **no workers** | `Archived`, `TargetActiveClusterId = stray-new`, Coordinator | `StrayOld` |
| `stray-new` | `Active`, no workers | `Active`, Coordinator + 1 Execution worker (`pg-agent-stray-new`) | `StrayNew` / `StrayNewAgent` |

Both clusters live in one container (`pg-stray`): the migrate runners resolve the target via
`JobMasterClusterAwareComponentFactories.TryGetFactory`, which only finds clusters registered in the same process.
Phase 3 reuses the container name, so the runner replaces phase 2's container. `api.json` also registers both
clusters (no workers), for the assertions.

**Phase 2** (`PostgresPurePhase2Emulator`) stages the race:
1. Stop `pg-archive` (phase 1's clusters aren't involved).
2. Schedule 20 `fast` jobs and one `TimeSpanInterval` (`00:00:15`) recurring schedule on `stray-old`. With no
   Coordinator, nothing moves them, so they stay there.
3. Stop `pg-stray`, then switch `stray-old`'s stored `ClusterMode` to `Archived` directly in the `StrayOld`
   database (`jm_generic_record_entry_value`, key `ClusterMode`, group `ClusterConfiguration`). That's the state
   the race leaves: the writes passed the mode check, then the switch landed.
4. Ids and test identifiers go to `PostgresPureState` for phase 3.

**Phase 3** (`PostgresPurePhase3Emulator`):
1. `pg-stray` restarts with `stray-old` configured as `Archived` with a target. Startup must not refuse it: an
   archived cluster with a target skips the unfinished-data check.
2. Wait (up to 7 min) until `stray-old` holds none of the 20 jobs and not the schedule.
3. Wait for all 20 executions, then assert they're `Succeeded` on `stray-new` with the same ids.
4. Assert the schedule is on `stray-new`, same id, `Active`, and that it fires once there. Then cancel it.

Not covered here (unit tests in `MigrateRunnersTests` cover them): an archived cluster without a target, or with a
target that isn't configured in the process (Critical log, nothing moved), and the 2.5-min vs 30-s intervals.
Jobs left in buckets of dead workers (moved once their processing deadline passes) aren't covered either: staging
them would add the 10-minute deadline plus a sweep to the run.

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
`PostgresPure`'s Phase2 DataRetentionTtl work already established). Expect phase 1 to take
roughly 12-18 minutes; `ArchiveWaitTimeout` (20 min) has headroom above that. Phases 2–3 add about 5–10 minutes
(the first sweep runs right after startup); the full run took 27 minutes on 2026-10-31.
