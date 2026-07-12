# Jwt

Same multi-cluster mechanism as [NoAuth](../NoAuth/README.md), but proves the JWT bearer auth path
instead — including the "get a token, then use it" mechanism itself, not just enforcement.

## Topology

- `jwt-cluster-one`, `jwt-cluster-two` — two `TargetTestScheduleApp` containers, each its own
  standalone cluster.
- `api` — one `TargetTestApi` container registering *both* clusters (zero workers).
- All three share the `PostgresStandalone` database.

## Auth

JWT bearer. The `api` container generates its own signing key at startup (never leaves the
container) and exposes `POST /auth/token`, which mints a token for a given subject. The test calls
that endpoint itself to get a token, then attaches it as `Authorization: Bearer <token>` on every
subsequent API call — unlike the other three scenarios, this credential isn't known ahead of
container start, so it can't be pre-attached as a default header.

## What the test does

`JwtTests.RunAllPhases` runs the shared `AuthApiPhase1EmulatorBase` logic (with
`JwtSubject = "scenario-tester"`, which triggers the token-fetch step):

1. Fetches a JWT from `/auth/token`.
2. Calls the API's cluster-list endpoint (authenticated via the JWT) and asserts both clusters are
   registered.
3. For each cluster: schedules a job directly against that cluster's own container, waits for the
   Redis execution record, then reads the job back through the API using the same JWT.
