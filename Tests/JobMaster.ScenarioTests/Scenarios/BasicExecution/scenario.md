# BasicExecution

Walking-skeleton scenario: proves the whole container/scheduling/assertion pipeline works
end-to-end before layering anything else on top.

## Topology

- `standalone` — one `TargetTestScheduleApp` container, cluster `basic-execution-cluster`.
- `api` — one `TargetTestApi` container, same cluster, zero workers (master-DB-only).
- Both share the `PostgresStandalone` database.

## Auth

`api.json` requires an API key (`x-api-key`, generated per run and attached automatically to
`Runner.Api`'s HTTP client).

## What the test does

`BasicExecutionTests.RunAllPhases` (via `BasicExecutionPhase1Emulator`):

1. Schedules one `fast` job.
2. Waits for exactly one execution record in Redis, then re-reads after a settle window to prove
   there was no duplicate delivery.
3. Cross-checks the same job's executions through the JobMaster API.
