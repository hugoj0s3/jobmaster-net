# SqlServerPure

SQL Server counterpart of ScheduleTest/PostgresPure: proves ~1000 jobs scheduled against a mix of
standalone and distributed clusters all execute exactly once — no losses, no duplicate delivery —
and that a job scheduled well beyond `TransientThreshold` is not dispatched early. Pure SQL Server
(no other repo type involved).

## Topology

- `sqlserver-standalone` — one `TargetTestScheduleApp` container, standalone cluster
  (`Standalone: true`, master + worker in one process), database `SqlServerStandalone`.
- `sqlserver-dist-one`, `sqlserver-dist-two` — two `TargetTestScheduleApp` containers, each a
  *distributed* cluster: a separate master DB (`SqlServerDistCluster`, shared by both) plus three
  agent connections (`ms-agent-1/2/3`, backed by
  `SqlServerAgent1`/`SqlServerAgent2`/`SqlServerAgent3`, also shared by both), each with its own
  worker.
- `api` — one `TargetTestApi` container, zero workers, registering all 3 clusters (master-DB-only)
  so job count/list queries can be cross-checked against the JobMaster API, not just Redis.
- Every cluster's `TransientThreshold` is `00:02:00` (2 minutes).
- All databases are shared across clusters, isolated only by `ClusterId`/agent-connection name —
  same pattern as PostgresPure and every other scenario in this suite.
- The shared SQL Server container runs as `sa` with a per-run generated password
  (`ScenarioGlobalEnvironment.SqlServerPassword`) — never hardcoded, never logged.

## What the test does

Identical plan/assertion logic to PostgresPure's `PostgresPurePhase1Emulator` (see
`../PostgresPure/scenario.md` for the full walkthrough), run against SQL Server instead:

1. Builds a `List<JobsQty>` plan up front — 150 `fast` + 50 `normal` + 3 `slow` jobs scheduled
   immediately, plus 100 `fast` + 30 `normal` jobs scheduled 5 minutes out, per cluster (999 jobs
   total across all 3 clusters).
2. Schedules every batch up front, recording each batch's own scheduled-at timestamp.
3. Asserts delayed batches show zero executions immediately, and again after all immediate work
   completes — proving `TransientThreshold` doesn't dispatch a 5-minute-out job early.
4. Waits out the remaining delay, then asserts each delayed batch executes in full, no earlier than
   its due time.
5. Cross-checks everything against both the Redis tracker and the JobMaster API
   (`page size = int.MaxValue`), per-cluster and per-handler-type.
