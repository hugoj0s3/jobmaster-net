# UserPass

Same multi-cluster mechanism as [NoAuth](../NoAuth/scenario.md), but proves the username/password
auth path instead.

## Topology

- `user-pass-cluster-one`, `user-pass-cluster-two` — two `TargetTestScheduleApp` containers, each
  its own standalone cluster.
- `api` — one `TargetTestApi` container registering *both* clusters (zero workers).
- All three share the `PostgresStandalone` database.

## Auth

Username/password headers (`X-User-Name` / `X-Password`), generated per run
(`ScenarioRunner.apiUsername`/`apiPassword`) and attached automatically to `Runner.Api`'s HTTP
client.

## What the test does

`UserPassTests.RunAllPhases` runs the shared `AuthApiPhase1EmulatorBase` logic:

1. Calls the API's cluster-list endpoint (authenticated via the username/password headers) and
   asserts both clusters are registered.
2. For each cluster: schedules a job directly against that cluster's own container, waits for the
   Redis execution record, then reads the job back through the API.
