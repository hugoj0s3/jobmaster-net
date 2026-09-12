# DrainModeTest.SqlServerDist

Tests each `AgentWorkerMode` (`Coordinator`, `Execution`, `Drain`) **in isolation, one per phase, each
in its own separate container**, under real load (5000 jobs), with a drain triggered while jobs are
genuinely in flight -- not after everything has already finished.

This is deliberately a different shape from `ScheduleTest`'s drain coverage (`Phase2`/`Phase3` there):
that suite bundles a Coordinator and 3 Drain workers into one shared container process, and only ever
drains an *idle* cluster (Phase1 fully completes before Phase2 flips any worker to `Drain`). Neither
of those choices exercises what happens when a worker dies with real work still in its buckets, and
bundling modes into one process can mask a bug in one runner behind another mode's runner picking up
the slack. This scenario keeps that from happening by construction.

This is the SQL Server mirror of `DrainModeTest.PostgresDist` -- same topology, same job plan, same
phase logic, only the repository type and connection strings differ.

## Topology

One cluster (`sqlserver-drain-load`), one master database, **3 agent connections**
(`sqlserver-agent-1/2/3`, each its own agent-side database) -- one per worker throughout, matching how
`ScheduleTest`'s own dist clusters are shaped, just split across separate containers/processes
instead of bundled into one:

| Phase | Containers | Modes |
|---|---|---|
| 1 | `sqlserver-coordinator`, `sqlserver-executor-1`, `sqlserver-executor-2`, `sqlserver-executor-3` | Coordinator, Execution x3 |
| 2 | `sqlserver-coordinator`, `sqlserver-drainer-1`, `sqlserver-drainer-2`, `sqlserver-drainer-3` | Coordinator, Drain x3 |
| 3 | `sqlserver-coordinator`, `sqlserver-executor-1`, `sqlserver-executor-2`, `sqlserver-executor-3` | Coordinator, Execution x3 |

Each connection keeps the same worker identity throughout: `sqlserver-executor-N` (Phase1) dies, is
drained by `sqlserver-drainer-N` (Phase2, same `AgentConnectionName: sqlserver-agent-N`), then
`sqlserver-executor-N` (Phase3, container name reused -- safe, since it was already explicitly stopped
in Phase1 before Phase2 ever starts) returns on the exact same connection. This mirrors a realistic
rolling crash/recycle-then-restore per connection, not an arbitrary pool sharing one connection with
no real reason to need 3 workers.

`sqlserver-coordinator` is re-declared unchanged in every phase for explicitness.

**`ScenarioRunner.StartPhaseAsync` only starts the containers a phase's `scenario.json` entry lists --
it does not stop a container just because a later phase omits it.** So all 3 Phase1 executors are
stopped *explicitly*, via `Runner.StopAsync(...)` at the end of `SqlServerDistPhase1Emulator`, not by
simply leaving them out of Phase2's container list. This is the simulated crash: the whole executor
fleet dies mid-batch, with most of the 5000 jobs still unfinished.

## A Coordinator is never truly execution-incapable -- found empirically, not a bug

The original design assumed Phase2 (Coordinator + Drain-only) would have zero execution capability,
and that Phase2 (Drain-only) vs Phase3 (real executors) shape a clean "before/after" split for job
completion. Real behavior is more interesting: `AssignJobsToBucketsRunner`
(`JobMaster/Sdk/Background/Runners/JobAndRecurringScheduleLifeCycleControl/AssignJobsToBucketsRunner.cs`,
part of every Coordinator's runner set) spins up its own embedded `PollingJobsExecutionRunner` for a
temporary **fallback bucket** once a job has had no regular bucket available for
`JobMasterConstants.NoBucketFallbackThreshold` (2.5 minutes) -- a deliberate job-starvation safety
net (see `ChangeLog.internal.md`'s "Coordinator fallback bucket durability" / "Orphaned fallback
buckets after a Coordinator crash" entries for the fix history on this exact mechanism). At 5000-job
volume the drain itself takes longer than 2.5 minutes, so this reliably triggers -- the Coordinator
*will* trickle some jobs through to `Succeeded` on its own during Phase2, slowly and serially.

This is the same underlying JobMaster mechanism `PostgresDist` observed -- provider-agnostic, not
something specific to SQL Server. `SqlServerDistPhase2Emulator` doesn't fight this, and doesn't wait
for it either, for the same reasons documented there: the fallback bucket lives on its own separate
reserved connection (`JobMasterConstants.MasterFallbackAgentConnName`), entirely independent of
`sqlserver-agent-1/2/3`, and is served by exactly one serial runner -- at 5000-job volume that trickle
simply cannot finish within any timeout reasonable for this phase, and it doesn't need to: finishing
everything is Phase3's job. So Phase2 asserts something narrower and connection-scoped instead: no
job is ever lost (total count stays at 5000 throughout), and every real bucket on all 3 connections
eventually gets destroyed by the drain lifecycle -- regardless of how far the fallback trickle has
gotten in the meantime. It does **not** wait for or assert zero execution during Phase2. Phase3 still
earns its place: it proves *fast, parallel* finalization via 3 live executors, in contrast to
Phase2's slow single-threaded fallback trickle -- genuinely different code paths
(`LoadExecutionRunners`'s normal `JobsExecutionRunner` vs. the Coordinator's one-off fallback
`PollingJobsExecutionRunner`), not just "the same thing, faster." Phase3's `FinalizeTimeout` also has
to absorb whatever the fallback runner left mid-flight (jobs sitting `Onboarded`/`Queued`/`InBucket`
in the fallback bucket recover independently via `HeldOnMasterDeadlineTimeoutJobsRunner` once their
`ProcessDeadline` passes, and get picked up again by a real bucket) -- this is real, self-healing
JobMaster behavior, not something the test needs to special-case.

## Job plan

5000 jobs (3500 `fast` / 1250 `normal` / 250 `slow`, `TestApp.Fast`/`Normal`/`Slow`, priorities
Medium/High/Low respectively) -- deliberately above `JobMasterDefaults.Worker.TransferBatchSize`
(1000, the number of jobs pulled from master per DB round-trip), so onboarding the full batch
requires multiple transfer cycles, not one. `DrainModeTestPlan.cs` (one directory up, shared with
`PostgresDist` and every other provider variant) is the single source of truth for this plan -- each
phase is a separate `Activator.CreateInstance` instance with no in-memory hand-off from the one
before it, so every phase recomputes the same deterministic (non-GUID) `TestIdentifier`s
(`drainload-fast`/`drainload-normal`/`drainload-slow`) instead of relying on state Phase1 alone would
hold. Job-set correctness across phases is verified against the API (persists across every container
restart), not any list passed between phases.

## Phase-by-phase

- **Phase1**: schedules all 5000 jobs (via `sqlserver-executor-1`'s HTTP endpoint -- which container
  issues the scheduling call doesn't matter, all 3 register the same cluster), then **immediately**
  stops all 3 executors -- no waiting, no assertions in between. This is deliberate: waiting for jobs
  to settle into any particular state before crashing (flushed off `PendingSave`, onboarded off
  `OnMaster`, etc.) would mean the crash only ever hits jobs already sitting in a bucket, exercising
  just `PollingDrainProcessingJobsRunner`. Crashing immediately after scheduling means jobs are
  caught across every pipeline stage -- some still `PendingSave` (agent-side transport only, not yet
  in the master store), some `OnMaster`, some already `InBucket`/`Onboarded`/`Queued`/`Processing` --
  so Phase2 has to recover through both `PollingDrainSavePendingJobsRunner` and
  `PollingDrainProcessingJobsRunner`, not just one of them.
- **Phase2**: checks only one thing, fast -- every job made it durably into the master store,
  regardless of status (`GetJobCountAsync(clusterId) == 5000`). It deliberately does **not** wait for
  the bucket lifecycle to finish draining/destroying the old buckets: that's a slow (tens of minutes),
  separate administrative concern with no bearing on job safety. It does capture the exact set of
  bucket IDs Phase1's dead executors owned (`DrainModeTestState.OriginalBucketIds`, populated right at
  the start of `RunAsync`, before anything else could create a new bucket on the same connections) so
  Phase3 can assert precisely those buckets' lifecycle later, unaffected by whatever fresh buckets
  Phase3's returning executors create on the same connections. Also does **not** assert anything about
  job status (`OnMaster` vs `Succeeded`) -- see the fallback-bucket section above for why that would be
  asserting something false about real JobMaster behavior.
- **Phase3**: live executors return on their original connections, onboard and finish the `OnMaster`
  backlog (plus whatever the Phase2 fallback trickle left mid-flight). Waits for all 5000 jobs to
  reach `Succeeded`, per batch, and cross-checks the Redis-tracked execution set against the
  API-persisted job set exactly -- no loss, no duplicate execution. Only *after* the job assertions
  pass does it check the bucket lifecycle, and only lightly: every one of Phase1's captured original
  bucket IDs must have reached `ReadyToDelete` (or already be physically gone) -- not full destruction.
  See "Bucket lifecycle: how far is far enough?" below for why.

## A real bug found and fixed along the way

This is historical context carried over from building `PostgresDist` -- the fixes are in framework
code shared by every repository provider, so they apply here too, not something this SQL Server
variant needed to rediscover.

Building the original `PostgresDist` scenario surfaced a genuine bug, not just a test-design lesson:
`MarkBucketAsLostRunner` (the periodic sweep that detects a dead worker's orphaned buckets) called
`WorkerClusterOperations.MarkBucketAsLostAsync(string bucketId)`, which re-fetched the bucket via a
*cached* read (`Get(bucketId, BucketFastAllowDiscrepancy)`) before marking it lost and writing it
back -- unlike its sibling `MarkBucketAsLostIfNotDrainingAsync`, already fixed earlier the same
release to use a fresh, uncached read. Confirmed live via direct container-log instrumentation (a
worker correctly showed `IsAlive=False`, the bucket was correctly identified as eligible, yet the
mark-as-lost write never reliably landed). Fixed by having `MarkBucketAsLostRunner` pass the
already-fresh `BucketModel` it already holds directly (no re-fetch at all), and by fixing the
`string`-keyed overload itself (still needed by `NatsJetStreamRunnerBase`, which only has an ID on
hand) to do a fresh `QueryAsync` re-read instead of the cached `Get`.

Also found and fixed: `DrainRunnersCoordinator.OnTickAsync` took a cluster-wide distributed lock
(`JobMasterLockKeys.BucketRunnerLock()`, shared with `MarkBucketAsLostRunner`,
`AssignedLostBucketsRunner`, and `DestroyReadyToDeleteBucketsRunner`) for no correctness reason.
`DrainRunnersCoordinator` runs every 10 seconds on *each* drainer (the highest-frequency contender by
far), yet its own bucket queries are already filtered to
`AgentWorkerId == BackgroundAgentWorker.AgentWorkerId` -- different drainers never touch each other's
buckets -- and every `JobMasterRunner` subclass with `useSemaphore: true` already serializes against
every other such runner *on the same worker process* via `BackgroundAgentWorker.MainSemaphoreSlim`,
so no cross-runner distributed lock was needed here at all, not even a worker-scoped one. Removed the
lock entirely rather than narrowing its scope. `AssignedLostBucketsRunner` and `MarkBucketAsLostRunner`
were left on the shared global lock -- both genuinely scan cluster-wide (not per-worker), so
de-contending them isn't the same easy, obviously-safe change.

A second bug surfaced alongside this: `WorkerClusterOperations.MarkBucketAsLostAsync(string bucketId)`
re-fetched the bucket via an awkward `QueryAsync(BucketIds: [bucketId])` (itself a workaround for
there being no dedicated "get one, uncached" method). Added
`IMasterBucketsService.GetNoCacheAsync(string bucketId)` as the proper primitive and switched both
`MarkBucketAsLostAsync` overloads to use it.

## Bucket lifecycle: how far is far enough?

The lifecycle tail (`Draining -> ReadyToDelete -> destroyed`) is gated by the same
`JobMasterConstants.BucketNoJobsBeforeReadyToDelete` (10 min) constant twice over:
`MarkBucketReadyToDeleteRunner` polls every 5 min and requires a continuous 10-min "no jobs" window
before transitioning to `ReadyToDelete` (~10-15 min realistically); `DestroyReadyToDeleteBucketsRunner`
itself only ticks every 10 min and won't act until the bucket's `DeletesAt` (`ReadyToDelete`-time + 10
more min) has passed (~10-20 min more). The two legs are comparable in duration -- reaching
`ReadyToDelete` is not a dramatic shortcut versus full physical deletion.

Given that, and that the `ReadyToDelete -> destroyed` mechanics are already covered in isolation by
`DestroyReadyToDeleteBucketsRunnerTests`, Phase3 only waits for Phase1's original buckets to reach
`ReadyToDelete` (or already be destroyed), not full deletion -- this scenario's job is to prove no job
was lost or duplicated under real drain/recover load, not to re-prove destruction timing. Phase2 also
does not wait on bucket state at all (see Phase2 above) -- it only captures the original bucket IDs for
Phase3 to check later, once the job assertions are already satisfied.

## Timing

Expected comparable to `PostgresDist`'s timing, not yet measured for this variant. For reference,
`PostgresDist`'s full passing run (5000 jobs, 3 phases, crashing immediately post-schedule so jobs are
caught across every pipeline stage -- see Phase1 above) took **1h4m** with Phase3 waiting for full
bucket destruction and widened `FinalizeTimeout` (45 min, to absorb real contention between the
Coordinator's shared runner semaphore serving new-job onboarding and still-active old-bucket cleanup
concurrently -- Phase2 no longer waits for the old buckets to fully drain before Phase3 starts).
Phase2 itself was fast (~1-2 min) since it only waits for durable save, not bucket drain. Relaxing
Phase3's bucket check from "destroyed" to "reached `ReadyToDelete`" (`OldBucketsResolvedTimeout` 60 ->
20 min) should meaningfully shorten the tail without losing signal -- most of that window elapses in
parallel with the `FinalizeTimeout` wait anyway. This variant carries the same relaxed timeouts
(`FinalizeTimeout` 45 min, `OldBucketsResolvedTimeout` 20 min) up front rather than starting from the
original unrelaxed values, since SQL Server has no reason a priori to behave meaningfully differently
from Postgres here. Re-tighten or loosen the timeouts if a real run's timing drifts materially from
what's encoded here.
