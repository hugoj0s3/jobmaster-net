# ApiKey

Same multi-cluster mechanism as [NoAuth](../NoAuth/README.md), but proves the API-key auth path
instead.

## Topology

- `api-key-cluster-one`, `api-key-cluster-two` — two `TargetTestScheduleApp` containers, each its
  own standalone cluster.
- `api` — one `TargetTestApi` container registering *both* clusters (zero workers).
- All three share the `PostgresStandalone` database.

## Auth

API key (`x-api-key`), generated per run (`ScenarioRunner.apiKey`) and attached automatically to
`Runner.Api`'s HTTP client.

## What the test does

`ApiKeyTests.RunAllPhases` runs the shared `AuthApiPhase1EmulatorBase` logic:

1. Calls the API's cluster-list endpoint (authenticated via the API key) and asserts both clusters
   are registered.
2. For each cluster: schedules a job directly against that cluster's own container, waits for the
   Redis execution record, then reads the job back through the API.
