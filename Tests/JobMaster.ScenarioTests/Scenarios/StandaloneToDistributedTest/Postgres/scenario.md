# StandaloneToDistributedTest.Postgres

Tests migrating a cluster off `Standalone` mode onto a genuinely distributed topology (separate
Coordinator + Execution containers) end to end: jobs stuck on the dead standalone worker's buckets
must drain and finish on the new topology, none lost, none duplicated. First pass, Postgres only,
two phases.

## Topology

**Same `ClusterId`, same database, two unrelated container sets across phases** — `pg-standalone`
in Phase1 is stopped explicitly (never re-listed in Phase2's `scenario.json` entry, and
`ScenarioRunner` never auto-stops a container just because a later phase omits it).

| Phase | Container(s) | Config | Notes |
|---|---|---|---|
| 1 | `pg-standalone` | `"Standalone": true`, one worker `w1` | All-in-one: Coordinator and Execution combined, using the reserved `JMReserved-standalone` connection (same connection string as master). |
| 2 | `pg-coordinator` + `pg-executor` | Ordinary Active cluster, Coordinator-only + Execution-on-`pg-agent-dist` | A brand new, real, named agent connection — never the reserved standalone one (a non-standalone worker may never declare that name explicitly). |

## Flow

1. Schedule 300 `fast` jobs against `pg-standalone`, then immediately stop it — no waiting. Same
   rationale as `DrainModeTest`'s Phase1: crashing immediately (rather than waiting for jobs to
   settle into one particular state) means recovery has to handle jobs caught at every stage of the
   pipeline — still `PendingSave` (dispatched to the bucket's agent-side message queue but not yet
   flushed to `jm_job`), still `OnMaster` (never onboarded), or already `InBucket`/`Processing`.
2. `pg-coordinator` + `pg-executor` start against the same `ClusterId`/database, `Standalone`
   omitted (non-standalone).
3. Wait for all 300 jobs to reach `Succeeded`, then assert the exact job ID set matches what
   `ScheduleAsync` originally returned — no loss, no duplicate execution across the topology change.
4. Assert `pg-agent-dist` is alive.

## The recovery mechanism (why this works)

`JobMasterRuntime.StartAsync` auto-synthesizes a `"StandaloneDrainer"` worker (`Mode = Drain`,
`AgentConnectionName = JMReserved-standalone`) whenever a non-standalone start finds buckets still
tagged as standalone-owned (`BucketModel.IsStandaloneBucket`) — one per still-present `WorkerLane`.
Because the reserved standalone connection points at the *same physical database* as master, this
drainer becomes a live worker on the *exact* `AgentConnectionId` the dead standalone worker's
buckets are pinned to, which is what makes ordinary drain machinery apply:
`MarkBucketAsLostRunner` (2.5 min tick) marks the orphaned bucket `Lost` →
`AssignedLostBucketsRunner` (1 min tick) finds the synthesized drainer alive on that same
connection and marks it `ReadyToDrain` → the drainer's own drain runners
(`PollingDrainSavePendingJobsRunner`/`PollingDrainProcessingJobsRunner`) push in-flight jobs back
to `OnMaster` → `AssignJobsToBucketsRunner` (10s probe) picks them up and assigns them to the new
cluster's real bucket on `pg-agent-dist`. This is the *only* recovery path for jobs still sitting
in the agent-side message queue (`PendingSave`) — `HeldOnMasterDeadlineTimeoutJobsRunner`'s
10-minute `ProcessDeadline` backstop only reclaims jobs already durably in `jm_job`, so if the
drainer synthesis didn't fire, `PendingSave` jobs would have no recovery path at all.

**The synthesis check re-evaluates live bucket state on every startup** (a plain
`IMasterBucketsService.QueryAllNoCacheAsync()` query, not a persisted "was this cluster ever
standalone" flag) — verified directly: killing the distributed process mid-drain (zero progress
made) and restarting it still correctly re-synthesizes the drainer and completes the drain, since
the standalone-tagged buckets are still there to be found. Not vulnerable to "interrupted drain
loses jobs forever" on a Coordinator restart.

## Real bug found and fixed building this scenario

**`ClusterDefinition.IsStandalone` (`bool?`) was never explicitly set to `false`** by
`ClusterConfigBuilder.ApplyJsonConfig`'s non-standalone branch — only the `Standalone: true` branch
ever assigned it. Since `JobMasterRuntime.cs` computes
`isStandalone = clusterDefinition.IsStandalone ?? modelToSave.IsStandalone`, a `null` here silently
deferred to whatever was *persisted* from an earlier run — meaning a cluster once configured
standalone could never be reconfigured back via JSON: `isStandalone` stayed `true` forever, the
`if (!isStandalone) { ... }` synthesis block was unreachable, and jobs stuck in `PendingSave` had
no recovery path (100% reproducible — "Timed out waiting for 300 executions. Observed 0" every
time). Fixed with one line, `clusterDefinition.IsStandalone = false;`, in the non-standalone branch.
Diagnosed via a fast native-process repro (running `TargetTestScheduleApp.dll` directly against a
real Postgres container, bypassing Docker-container/image-build overhead) rather than the ~20-minute
full scenario cycle — cut iteration time from ~20 minutes to under a minute. Regression tests:
`Tests/JobMaster.UnitTests/Sdk/Ioc/Setup/ClusterConfigBuilderStandaloneTests.cs`,
`Sdk/Abstractions/Models/Buckets/BucketModelTests.cs`,
`Sdk/Abstractions/Models/Buckets/StandaloneDrainerSynthesisConditionTests.cs`.

## What this doesn't cover (deliberately, first pass)

- **Target cluster's own bucket lifecycle** — doesn't assert the old standalone buckets ever reach
  `ReadyToDelete`/get destroyed (that's already covered in isolation elsewhere, e.g.
  `DestroyReadyToDeleteBucketsRunnerTests`); this scenario's job is to prove no job was lost or
  duplicated across the transition, not to re-prove bucket destruction timing.
- **MySql/SqlServer/PostgresNats** — Postgres only, matching this suite's other scenarios' first-pass
  approach.

## Timing

First real run: 8m45s end to end (schedule + crash + startup + drain + onboard + execute 300
jobs) — well under the 20-minute `FinalizeTimeout`, confirming the fast auto-drain path dominates
in practice rather than the slower `ProcessDeadline` backstop.
