# DrainModeTest.PostgresNats

Tests each `AgentWorkerMode` (`Coordinator`, `Execution`, `Drain`) **in isolation, one per phase, each
in its own separate container**, under real load (5000 jobs), with a drain triggered while jobs are
genuinely in flight — not after everything has already finished. Mixed providers, mirroring
`ScheduleTest.PostgresNats`'s pairing: Postgres for master/cluster storage, NATS JetStream for the
agent/transport layer (job dispatch queue) on all 3 agent connections. This is the NATS-transport twin
of `DrainModeTest.PostgresDist` — same topology, same job plan, same phase structure — swapping only
the agent connections' `RepositoryType`.

This is deliberately a different shape from `ScheduleTest`'s drain coverage (`Phase2`/`Phase3` there):
that suite bundles a Coordinator and 3 Drain workers into one shared container process, and only ever
drains an *idle* cluster (Phase1 fully completes before Phase2 flips any worker to `Drain`). Neither
of those choices exercises what happens when a worker dies with real work still in its buckets, and
bundling modes into one process can mask a bug in one runner behind another mode's runner picking up
the slack. This scenario keeps that from happening by construction — same reasoning as
`DrainModeTest.PostgresDist`, just exercised against NATS JetStream's own drain-runner implementations
(`NatsJetStreamDrainSavePendingJobsRunner`, `NatsJetStreamDrainProcessingRunner`) instead of the
generic SQL-backed ones.

## Topology

One cluster (`postgres-nats-drain-load`), one Postgres master database
(`PostgresNatsDrainLoadCluster`), **3 NATS JetStream agent connections** (`nats-agent-1/2/3`) — one
per worker throughout. Unlike the SQL providers (where each agent connection gets its own database),
all 3 agent connections here share the same physical NATS server/connection string; NATS has no
per-connection database concept, so partitioning happens internally via JetStream stream naming
instead (`NatsJetStreamUtils`, namespaced by agent connection id) — same pattern established by
`ScheduleTest.PostgresNats`.

| Phase | Containers | Modes |
|---|---|---|
| 1 | `postgres-nats-coordinator`, `postgres-nats-executor-1`, `postgres-nats-executor-2`, `postgres-nats-executor-3` | Coordinator, Execution ×3 |
| 2 | `postgres-nats-coordinator`, `postgres-nats-drainer-1`, `postgres-nats-drainer-2`, `postgres-nats-drainer-3` | Coordinator, Drain ×3 |
| 3 | `postgres-nats-coordinator`, `postgres-nats-executor-1`, `postgres-nats-executor-2`, `postgres-nats-executor-3` | Coordinator, Execution ×3 |

Each connection keeps the same worker identity throughout: `postgres-nats-executor-N` (Phase1) dies,
is drained by `postgres-nats-drainer-N` (Phase2, same `AgentConnectionName: nats-agent-N`), then
`postgres-nats-executor-N` (Phase3, container name reused — safe, since it was already explicitly
stopped in Phase1 before Phase2 ever starts) returns on the exact same connection. This mirrors a
realistic rolling crash/recycle-then-restore per connection, not an arbitrary pool sharing one
connection with no real reason to need 3 workers.

`postgres-nats-coordinator` is re-declared unchanged in every phase for explicitness. It never
declares a NATS agent connection of its own (a Coordinator worker never has one), so it stays on
plain Postgres master access throughout, same as every other container.

**`ScenarioRunner.StartPhaseAsync` only starts the containers a phase's `scenario.json` entry lists —
it does not stop a container just because a later phase omits it.** So all 3 Phase1 executors are
stopped *explicitly*, via `Runner.StopAsync(...)` at the end of `PostgresNatsPhase1Emulator`, not by
simply leaving them out of Phase2's container list. This is the simulated crash: the whole executor
fleet dies mid-batch, with most of the 5000 jobs still unfinished.

Every cluster config block across all 3 phases — including the coordinator's, which has no NATS
agent connection of its own — sets `"TransientThreshold": "00:05:00"` explicitly. Only the
executor/drainer configs, which declare `RepositoryType: "NatsJetStream"` agent connections, are
actually subject to `NatsJetStreamJobMasterRuntimeSetup.ValidateAsync`'s cap (it only inspects a
process's own `AgentConnections`, so the coordinator's declared value is never checked); it's set on
every block anyway to keep one consistent value across all workers of the same cluster, matching
`pg-coordinator`'s "re-declared unchanged... for explicitness" precedent. This value is required
because `JobMasterDefaults.TransientThreshold` (the SDK-wide default, confirmed in
`JobMaster/Abstractions/JobMasterDefaults.cs`) is **10 minutes**, which exceeds
`NatsJetStreamConstants.MaxThreshold` (5 minutes) and would fail cluster startup outright for any
config that declares a NATS agent connection without an explicit override. `DrainModeTest.PostgresDist`
never sets this field at all (its 3 agent connections are all Postgres, so the 10-minute default is
fine there) — this is the one config difference this variant *must* add, not a style choice.

## Infrastructure

Same lazily-started, run-scoped-singleton NATS+JetStream Testcontainer as every other `*Nats`
scenario in this suite (`ScenarioGlobalEnvironment.GetOrStartNatsAsync` — one NATS server for the
whole test run, network alias `nats`, port `4222`, credentials generated once per run). No extra
provisioning step is needed the way `PostgresDatabaseProvisioner` is for the SQL providers —
`NatsJetStreamJobMasterRuntimeSetup` provisions streams itself inside each app container at startup.
`ScenarioRunner.EnsureDatabasesForContainerAsync` already auto-detects and starts the NATS container
whenever a rendered cluster config's `RepoType` or any `AgentConnections[].RepositoryType` is
`"NatsJetStream"` — same mechanism `ScheduleTest.PostgresNats` relies on, no new plumbing needed for
this variant.

## A Coordinator is never truly execution-incapable — found empirically, not a bug

The original design assumed Phase2 (Coordinator + Drain-only) would have zero execution capability,
and that Phase2 (Drain-only) vs Phase3 (real executors) shape a clean "before/after" split for job
completion. Real behavior is more interesting: `AssignJobsToBucketsRunner`
(`JobMaster/Sdk/Background/Runners/JobAndRecurringScheduleLifeCycleControl/AssignJobsToBucketsRunner.cs`,
part of every Coordinator's runner set) spins up its own embedded `PollingJobsExecutionRunner` for a
temporary **fallback bucket** once a job has had no regular bucket available for
`JobMasterConstants.NoBucketFallbackThreshold` (2.5 minutes) — a deliberate job-starvation safety
net (see `ChangeLog.internal.md`'s "Coordinator fallback bucket durability" / "Orphaned fallback
buckets after a Coordinator crash" entries for the fix history on this exact mechanism, discovered
while building `DrainModeTest.PostgresDist`). At 5000-job volume the drain itself takes longer than
2.5 minutes, so this reliably triggers here too — the Coordinator *will* trickle some jobs through to
`Succeeded` on its own during Phase2, slowly and serially.

`PostgresNatsPhase2Emulator` doesn't fight this, and doesn't wait for it either, for the same reasons
as its `PostgresDist` counterpart. The fallback bucket lives on its own separate reserved connection
(`JobMasterConstants.MasterFallbackAgentConnName`), entirely independent of `nats-agent-1/2/3`, and is
served by exactly one serial runner — at 5000-job volume that trickle simply cannot finish within any
timeout reasonable for this phase, and it doesn't need to: finishing everything is Phase3's job. So
Phase2 asserts something narrower and connection-scoped instead: no job is ever lost (total count
stays at 5000 throughout), and every real bucket on all 3 connections eventually gets destroyed by
the drain lifecycle — regardless of how far the fallback trickle has gotten in the meantime. It does
**not** wait for or assert zero execution during Phase2. Phase3 still earns its place: it proves
*fast, parallel* finalization via 3 live executors, in contrast to Phase2's slow single-threaded
fallback trickle — genuinely different code paths, not just "the same thing, faster." Phase3's
`FinalizeTimeout` also has to absorb whatever the fallback runner left mid-flight (jobs sitting
`Onboarded`/`Queued`/`InBucket` in the fallback bucket recover independently via
`HeldOnMasterDeadlineTimeoutJobsRunner` once their `ProcessDeadline` passes, and get picked up again
by a real bucket) — this is real, self-healing JobMaster behavior, not something the test needs to
special-case.

## Job plan

5000 jobs (3500 `fast` / 1250 `normal` / 250 `slow`, `TestApp.Fast`/`Normal`/`Slow`, priorities
Medium/High/Low respectively) — deliberately above `JobMasterDefaults.Worker.TransferBatchSize`
(1000, the number of jobs pulled from master per DB round-trip), so onboarding the full batch
requires multiple transfer cycles, not one. `DrainModeTestPlan.cs` (one directory up, shared and
unchanged across every `DrainModeTest` provider variant) is the single source of truth for this plan,
shared by all 3 phases — each phase is a separate `Activator.CreateInstance` instance with no
in-memory hand-off from the one before it, so every phase recomputes the same deterministic
(non-GUID) `TestIdentifier`s (`drainload-fast`/`drainload-normal`/`drainload-slow`) instead of relying
on state Phase1 alone would hold. Job-set correctness across phases is verified against the API
(persists across every container restart), not any list passed between phases.

## Phase-by-phase

- **Phase1**: schedules all 5000 jobs (via `postgres-nats-executor-1`'s HTTP endpoint — which
  container issues the scheduling call doesn't matter, all 3 register the same cluster), then
  **immediately** stops all 3 executors — no waiting, no assertions in between. This is deliberate:
  waiting for jobs to settle into any particular state before crashing (flushed off `PendingSave`,
  onboarded off `OnMaster`, etc.) would mean the crash only ever hits jobs already sitting in a
  bucket, exercising just `NatsJetStreamDrainProcessingRunner`. Crashing immediately after scheduling
  means jobs are caught across every pipeline stage — some still `PendingSave` (agent-side transport
  only, not yet in the master store), some `OnMaster`, some already `InBucket`/`Onboarded`/`Queued`/
  `Processing` — so Phase2 has to recover through both `NatsJetStreamDrainSavePendingJobsRunner` and
  `NatsJetStreamDrainProcessingRunner`, not just one of them.
- **Phase2**: checks only one thing, fast — every job made it durably into the master store,
  regardless of status (`GetJobCountAsync(clusterId) == 5000`). It deliberately does **not** wait for
  the bucket lifecycle to finish draining/destroying the old buckets: that's a slow (tens of minutes),
  separate administrative concern with no bearing on job safety. It does capture the exact set of
  bucket IDs Phase1's dead executors owned (`DrainModeTestState.OriginalBucketIds`, populated right at
  the start of `RunAsync`, before anything else could create a new bucket on the same connections) so
  Phase3 can assert precisely those buckets' lifecycle later, unaffected by whatever fresh buckets
  Phase3's returning executors create on the same connections. Also does **not** assert anything about
  job status (`OnMaster` vs `Succeeded`) — see the fallback-bucket section above for why that would be
  asserting something false about real JobMaster behavior.
- **Phase3**: live executors return on their original connections, onboard and finish the `OnMaster`
  backlog (plus whatever the Phase2 fallback trickle left mid-flight). Waits for all 5000 jobs to
  reach `Succeeded`, per batch, and cross-checks the Redis-tracked execution set against the
  API-persisted job set exactly — no loss, no duplicate execution. Only *after* the job assertions
  pass does it check the bucket lifecycle, and only lightly: every one of Phase1's captured original
  bucket IDs must have reached `ReadyToDelete` (or already be physically gone) — not full destruction.
  See "Bucket lifecycle: how far is far enough?" below for why.

## Framework bugs found while building the template (historical context)

`DrainModeTest.PostgresDist` (this scenario's template) surfaced two real SDK bugs while it was being
built: `MarkBucketAsLostRunner` re-fetching a bucket via a cached read before marking it lost (fixed
by passing the already-fresh `BucketModel` directly, plus a fresh-read fix to the `string`-keyed
overload still used by `NatsJetStreamRunnerBase`), and an unnecessary cluster-wide distributed lock in
`DrainRunnersCoordinator.OnTickAsync` (removed — its own queries are already worker-scoped, and
`BackgroundAgentWorker.MainSemaphoreSlim` already serializes same-process runners). Both fixes are
already in place and exercised by every `DrainModeTest` variant, including this one; see
`DrainModeTest/PostgresDist/scenario.md` for the full write-up. Nothing new was found while building
this NATS variant itself — it reuses the same emulator logic verbatim, just against NATS's own drain
runners.

## Bucket lifecycle: how far is far enough?

The lifecycle tail (`Draining → ReadyToDelete → destroyed`) is gated by the same
`JobMasterConstants.BucketNoJobsBeforeReadyToDelete` (10 min) constant twice over:
`MarkBucketReadyToDeleteRunner` polls every 5 min and requires a continuous 10-min "no jobs" window
before transitioning to `ReadyToDelete` (~10-15 min realistically); `DestroyReadyToDeleteBucketsRunner`
itself only ticks every 10 min and won't act until the bucket's `DeletesAt` (`ReadyToDelete`-time + 10
more min) has passed (~10-20 min more). The two legs are comparable in duration — reaching
`ReadyToDelete` is not a dramatic shortcut versus full physical deletion.

Given that, and that the `ReadyToDelete → destroyed` mechanics are already covered in isolation by
`DestroyReadyToDeleteBucketsRunnerTests`, Phase3 only waits for Phase1's original buckets to reach
`ReadyToDelete` (or already be destroyed), not full deletion — this scenario's job is to prove no job
was lost or duplicated under real drain/recover load, not to re-prove destruction timing. Phase2 also
does not wait on bucket state at all (see Phase2 above) — it only captures the original bucket IDs for
Phase3 to check later, once the job assertions are already satisfied.

## Timing

Not yet run. `DrainModeTest.PostgresDist`'s equivalent full run (5000 jobs, 3 phases) took **1h4m**
with the same `FinalizeTimeout` (45 min) and `OldBucketsResolvedTimeout` (20 min) values this variant
reuses unchanged. Expected to land in the same order of magnitude — this scenario's bottlenecks
(Coordinator runner-semaphore contention during concurrent onboarding/cleanup, the 10-minute
`BucketNoJobsBeforeReadyToDelete` bucket-lifecycle gate) are generic Sdk-level mechanics, not
transport-specific — but this has not been empirically confirmed for NATS JetStream's own drain
runners, which have different dispatch/ack characteristics than the SQL-backed ones. Re-tighten or
loosen the timeouts once a real run's timing is measured, the same way `PostgresDist`'s were tuned
from its own runs.
