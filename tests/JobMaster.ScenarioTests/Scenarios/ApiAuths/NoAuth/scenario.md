# NoAuth

Proves multiple distinct clusters can share one database without interfering with each other, and
that the JobMaster API works correctly with authentication turned off entirely.

## Topology

- `no-auth-cluster-one`, `no-auth-cluster-two` — two `TargetTestScheduleApp` containers, each its
  own standalone cluster.
- `api` — one `TargetTestApi` container registering *both* clusters (zero workers).
- All three share the `PostgresStandalone` database.

## Auth

None — `api.json` sets `requireAuthentication: false` and configures no credentials.

## What the test does

`NoAuthTests.RunAllPhases` runs the shared `AuthApiPhase1EmulatorBase` logic:

1. Calls the API's cluster-list endpoint and asserts both `no-auth-cluster-one` and
   `no-auth-cluster-two` are registered.
2. For each cluster: schedules a job directly against that cluster's own container (bypassing the
   YARP proxy, which would otherwise round-robin the request to the wrong cluster's container),
   waits for the Redis execution record, then reads the job back through the API.
