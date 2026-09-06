# StandaloneToDistributedTest.RavenDbPure

Tests migrating a cluster off `Standalone` mode onto a genuinely distributed topology (separate
Coordinator + Execution containers) end to end: jobs stuck on the dead standalone worker's buckets
must drain and finish on the new topology, none lost, none duplicated. Same test as `PostgresPure`,
`MySqlPure`, and `SqlServerPure`, run against the RavenDB provider instead of a SQL one.

## Topology

**Same `ClusterId`, same database, two unrelated container sets across phases** — `ravendb-standalone`
in Phase1 is stopped explicitly (never re-listed in Phase2's `scenario.json` entry, and
`ScenarioRunner` never auto-stops a container just because a later phase omits it).

| Phase | Container(s) | Config | Notes |
|---|---|---|---|
| 1 | `ravendb-standalone` | `"Standalone": true`, one worker `w1` | All-in-one: Coordinator and Execution combined, using the reserved `JMReserved-standalone` connection (same connection string as master). |
| 2 | `ravendb-coordinator` + `ravendb-executor` | Ordinary Active cluster, Coordinator-only + Execution-on-`ravendb-agent-dist` | A brand new, real, named agent connection — never the reserved standalone one (a non-standalone worker may never declare that name explicitly). |

No `ConnectionOptions` tuning (e.g. `pooledConnectionLifetimeMs`) is set on master (either container)
or the `ravendb-agent-dist` agent connection — plain connection strings, matching every other
RavenDB scenario variant in this suite. That binder behavior is already covered by unit tests
(`Tests/JobMaster.UnitTests/Ioc/RavenDbConnectionOptionsTests.cs`).

## Flow

1. Schedule 300 `fast` jobs against `ravendb-standalone`, then immediately stop it — no waiting. Same
   rationale as `DrainModeTest`'s Phase1: crashing immediately (rather than waiting for jobs to
   settle into one particular state) means recovery has to handle jobs caught at every stage of the
   pipeline — still `PendingSave` (dispatched to the bucket's agent-side message queue but not yet
   flushed to durable storage), still `OnMaster` (never onboarded), or already `InBucket`/`Processing`.
2. `ravendb-coordinator` + `ravendb-executor` start against the same `ClusterId`/database, `Standalone`
   omitted (non-standalone).
3. Wait for all 300 jobs to reach `Succeeded`, then assert the exact job ID set matches what
   `ScheduleAsync` originally returned — no loss, no duplicate execution across the topology change.
4. Assert `ravendb-agent-dist` is alive.

## Multi-variant note

`TestIdentifier` is generated fresh (`Guid.NewGuid()`) in Phase1 rather than a literal constant —
every repo-type variant shares one Redis instance (`ScenarioGlobalEnvironment`), and
`[Collection(ScenarioCollection.Name)]` runs them sequentially without clearing Redis in between, so
a literal identifier would let a later variant's `Tracker.WaitForAsync` (in Phase2) match an earlier
variant's already-recorded executions. Since Phase1 and Phase2 are separate
`Activator.CreateInstance` instances with no direct reference to each other, the generated
identifier (and the scheduled job IDs) are handed off via `RavenDbPureState`, a small static holder
scoped to this variant's own namespace — safe specifically because scenarios run within one
serialized collection, never concurrently with another scenario run in the same process.

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
cluster's real bucket on `ravendb-agent-dist`. This is the *only* recovery path for jobs still
sitting in the agent-side message queue (`PendingSave`) — `HeldOnMasterDeadlineTimeoutJobsRunner`'s
10-minute `ProcessDeadline` backstop only reclaims jobs already durably persisted, so if the drainer
synthesis didn't fire, `PendingSave` jobs would have no recovery path at all. This mechanism is
provider-agnostic — the recovery machinery described above is exercised identically regardless of
whether master/agent storage is RavenDB or a SQL repository.

**The synthesis check re-evaluates live bucket state on every startup** (a plain
`IMasterBucketsService.QueryAllNoCacheAsync()` query, not a persisted "was this cluster ever
standalone" flag) — see `PostgresPure/scenario.md` for the restart-mid-drain verification; the same
mechanism applies here unchanged, RavenDB is only the storage underneath it.

## What this doesn't cover (deliberately)

- **Target cluster's own bucket lifecycle** — doesn't assert the old standalone buckets ever reach
  `ReadyToDelete`/get destroyed (that's already covered in isolation elsewhere, e.g.
  `DestroyReadyToDeleteBucketsRunnerTests`); this scenario's job is to prove no job was lost or
  duplicated across the transition, not to re-prove bucket destruction timing.
- **No `RavenDbNats` variant** — this scenario's recovery path is entirely about master/agent
  storage (drain machinery reading/writing bucket and job state), not about the transport a worker
  uses to receive jobs; a NATS-backed agent connection wouldn't change any of the assertions here.
  Skipped deliberately, matching the same reasoning already used for
  `FallbackBucketTest.RavenDbPure`.

## Timing

Expect a runtime comparable to `PostgresPure`'s first real run (8m45s end to end: schedule + crash +
startup + drain + onboard + execute 300 jobs) — well under the 20-minute `FinalizeTimeout`.
