# PostgresPure

Proves ~1000 jobs scheduled against a mix of standalone and distributed clusters all execute
exactly once — no losses, no duplicate delivery — and that a job scheduled well beyond
`TransientThreshold` is not dispatched early. Pure Postgres (no other repo type involved).

## Topology

- `postgres-standalone` — one `TargetTestScheduleApp` container, standalone cluster
  (`Standalone: true`, master + worker in one process), database `PostgresStandalone`.
- `postgres-dist-one`, `postgres-dist-two` — two `TargetTestScheduleApp` containers, each a
  *distributed* cluster: a separate master DB (`PostgresDistCluster`, shared by both) plus three
  agent connections (`pg-agent-1/2/3`, backed by
  `PostgreAgent1`/`PostgreAgent2`/`PostgreAgent3`, also shared by both), each with its own worker.
- `api` — one `TargetTestApi` container, zero workers, registering all 3 clusters (master-DB-only)
  so job count/list queries can be cross-checked against the JobMaster API, not just Redis.
- Every cluster's `TransientThreshold` is `00:02:00` (2 minutes).
- All databases are shared across clusters, isolated only by `ClusterId`/agent-connection name —
  same pattern as every other scenario in this suite, now exercised with a real master-DB/agent-DB
  split instead of standalone's single-database shortcut.

## What Phase1 does

`PostgresPureTests.RunAllPhases` (via `PostgresPurePhase1Emulator` → the shared
`PureScheduleTestPhase1EmulatorBase`):

1. Builds a `List<JobsQty>` plan up front — for each of the 3 clusters: 150 `fast` + 50 `normal` +
   3 `slow` jobs scheduled immediately, plus 100 `fast` + 30 `normal` jobs scheduled 5 minutes out
   (`AfterSecs`). Each handler type also carries a fixed `Priority` (fast=Medium, normal=High,
   slow=Low). That's 333 jobs/cluster × 3 clusters = 999 jobs total. `verylong` (3 min/job) is
   excluded — a couple of those on the single-worker standalone cluster would eat the time budget
   this test relies on to stay well clear of the 5-minute delay window.
2. Schedules every batch in the plan up front (one HTTP call per batch, direct to that cluster's own
   container, bypassing the YARP proxy), recording each batch's own scheduled-at timestamp,
   `TestIdentifier`, and returned job ids — so the delayed batches' 5-minute clocks start immediately
   and every later assertion can be driven off the plan.
3. Immediately checks every delayed batch has zero executions in Redis — proving a job scheduled 5
   minutes out, on a cluster whose `TransientThreshold` is only 2 minutes, is not dispatched right
   away.
4. Waits for every immediate batch to fully execute (well within the 5-minute window), asserting the
   executed job id set matches exactly what was scheduled.
5. Re-checks every delayed batch is still at zero executions after all that immediate work.
6. Waits out whatever's left of each delayed batch's 5-minute delay, then asserts it executes in
   full — and that the earliest execution timestamp is not meaningfully earlier than its due time.
7. Re-reads every batch (delayed and immediate) after a settle window to prove no duplicate/late
   delivery, not just "at least N yet".
8. Cross-checks the same plan against the JobMaster API (`Runner.Api`, `page size = int.MaxValue` to
   bypass the API's default 25-item page) across four filter combinations — `ClusterId` alone,
   `ClusterId`+`JobDefinitionId`, `ClusterId`+`Priority`, and `ClusterId`+`TestIdentifier` (per
   individual scheduled batch, via the API's metadata filter, since `TestIdentifier` lives in job
   Metadata, not a first-class column) — asserting each returns exactly the expected job id set and
   that every job's `Status` is `Succeeded`.

## What Phase2 does

`PostgresPurePhase2Emulator` (via the shared `DataRetentionPhase2EmulatorBase`) reuses the ~999 jobs
Phase1 already scheduled and finished, and exercises two *independent* lifecycle mechanisms against
them: `DataRetentionTtl`-based job purge, and a real drain-to-completion bucket lifecycle. These are
deliberately kept on separate clusters (see below) rather than combined on one, since a cluster
whose only worker is Coordinator-mode never runs `DeleteOldFinalJobsRunner`'s prerequisite drain
runners in the same process as a Drain-mode worker unless both are present together.

- `postgres-standalone` — its container is stopped and restarted (same `ClusterId`/database) with
  `DataRetentionTtl: 00:10:00` added, worker unchanged. Standalone clusters can't run a
  Coordinator/Drain-mode worker at all (`IClusterStandaloneConfigSelector.AddWorker` has no
  `WorkerMode` parameter), but the TTL-purge runner (`DeleteOldFinalJobsRunner`) runs regardless of
  worker mode, so this cluster is purely a "does TTL purge work" check, and — since standalone can
  never be reconfigured into a Coordinator/Drain topology — it's also where a connection is
  effectively *never* deleted (that only ever happens on a distributed cluster).
- `postgres-dist-one` — no `DataRetentionTtl` at all; its purpose here is purely the
  connection-retirement lifecycle, decoupled from TTL purge. Its container is restarted with **four
  workers in one process**: a `Coordinator`-mode worker (no `AgentConnectionName` — a Coordinator
  worker must not have one, `JobMasterRuntime` throws if it does) plus its original 3 workers
  switched to `Drain` mode (still with their `AgentConnectionName`). This mirrors the only way
  JobMaster actually supports retiring a connection: `AssignedLostBucketsRunner` (which reassigns a
  dead worker's orphaned buckets to a live one) only runs under Coordinator/Full mode, never under a
  lone Drain worker — so a Coordinator has to be present to hand the original Full-mode worker's
  now-`Lost` buckets to the new Drain workers. Once reassigned, each Drain worker's own
  `DrainRunnersCoordinator` (real, unmodified machinery) takes it from there:
  `ReadyToDrain` → `Draining` → `ReadyToDelete` (after the existing 10-minute
  `BucketNoJobsBeforeReadyToDelete` no-jobs wait) → destroyed. Phase2 waits for this cluster's
  bucket count to reach zero — proving the real drain actually finished, not a shortcut.
- `postgres-dist-two` — restarted with the *same* config Phase1 used (no `DataRetentionTtl`, no
  worker-mode change) rather than left running untouched. The control case: proves
  `DataRetentionTtl` itself is what causes the purge, not merely an app-container restart, and that
  a cluster that never configured it keeps its jobs indefinitely.

`ScenarioRunner.StartContainerAsync` auto-stops a container by the same name before starting its
Phase2 replacement, so Phase1's already-populated database is preserved across every restart.

## What Phase3 does

`PostgresPurePhase3Emulator` (via the shared `DataRetentionPhase3EmulatorBase`) only touches
`postgres-dist-one` — the cluster whose buckets Phase2 already proved are fully drained and
destroyed. Its container is restarted a second time with **only the Coordinator worker** (the 3
Drain workers are gone entirely). This is the step that actually lets the connection go idle: a
Drain-mode worker keeps heartbeating its `AgentConnectionId` indefinitely
(`LoadAgentConnectionRunnersAsync` runs for every non-Coordinator mode) even once it has nothing
left to drain, so removing the Drain workers — not merely destroying their buckets — is what's
required. Phase3 asserts the connections go dead fast (`ApiAgentConnection.IsAlive` → `false`, via
`ResourceAliveThreshold`, ~45s) and then, much later, get physically deleted by
`CleanupDeadAgentConnectionsRunner`'s own hardcoded, non-configurable 30-minute dead threshold plus
its own 5-minute tick — only possible because Phase2 already emptied its buckets
(`HasBucketsAsync` gates `SafeDeleteConnectionAsync`).

This 3-phase sequence models the real, correct way to retire a JobMaster agent connection — drain
first, then remove the worker — rather than a shortcut. Runtime is dominated by Phase2's drain wait
(~20-30 min) and Phase3's connection-deletion wait (~30-40 min); expect roughly 60-70 minutes total
for Phase1+Phase2+Phase3.
