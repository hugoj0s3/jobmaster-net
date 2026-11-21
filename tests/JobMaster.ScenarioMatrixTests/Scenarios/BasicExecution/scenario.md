# BasicExecution

Walking-skeleton scenario: proves the whole container / scheduling / assertion pipeline works end to end on each
master provider. Matrix port of `JobMaster.ScenarioTests/Scenarios/BasicExecution` (Postgres only there).

## Topology
- `mx-<flavor>-basic-exec`: one `TargetTestScheduleApp` container, a **standalone** cluster of the same name (no
  agent connections; the master database does both).
- `api`: one `TargetTestApi` container on the same cluster and master database, no workers. Requires the per-run API
  key; JWT enabled.

## Flow
1. Schedule one `fast` job.
2. It runs exactly once (Redis tracker, re-read after a 2 s settle window).
3. The API shows its execution.

## Flavors
PostgresPure, MySqlPure, SqlServerPure, RavenDbPure. PostgresNats and HybridNatsRavenDb are excluded: a standalone
cluster has no agent connection, so they'd run the same as PostgresPure and RavenDbPure.

Run one flavor: `dotnet test tests/JobMaster.ScenarioMatrixTests --filter "FullyQualifiedName~BasicExecution&DisplayName~MySqlPure"`.
