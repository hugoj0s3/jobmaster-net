# FallbackBucketTest.SqlServerPure

Same as [`PostgresPure`](../PostgresPure/scenario.md), on SQL Server. See that scenario's
`scenario.md` for the full mechanism writeup — this file only notes what differs.

## What differs from PostgresPure

- `RepoType`/`RepositoryType`: `SqlServer`.
- Connection string carries `TrustServerCertificate=True;Max Pool Size=300;`.
- RCSI (`READ_COMMITTED_SNAPSHOT`) is enabled automatically by `SqlServerDatabaseProvisioner` for
  every SqlServer database this scenario's JSON declares — no extra JSON/code needed.
- No other logic differs — `FallbackBucketTestPhase1EmulatorBase` is shared verbatim across every
  repo-type variant of this scenario.

## What this doesn't cover (deliberately)

Same as `PostgresPure` — see that file. No `PostgresNats` variant either, for the same reason
(nothing for a NATS-backed agent connection to replace when the whole point is zero agent
connections).
