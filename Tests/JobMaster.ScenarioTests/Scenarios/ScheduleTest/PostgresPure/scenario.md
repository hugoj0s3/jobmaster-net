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

## What the test does

`PostgresPureTests.RunAllPhases` (via `PostgresPurePhase1Emulator`):

1. Builds a `List<JobsQty>` plan up front — for each of the 3 clusters: 150 `fast` + 50 `normal` +
   3 `slow` jobs scheduled immediately, plus 100 `fast` + 30 `normal` jobs scheduled 5 minutes out
   (`AfterSecs`). That's 333 jobs/cluster × 3 clusters = 999 jobs total. `verylong` (3 min/job) is
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
   bypass the API's default 25-item page): per-cluster job count and full job list, and per-cluster
   per-handler-type job count and filtered job list, both matched against the exact job id sets the
   plan expects.
