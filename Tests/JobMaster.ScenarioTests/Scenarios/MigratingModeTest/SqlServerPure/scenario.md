# MigratingModeTest.SqlServerPure

Same as [`PostgresPure`](../PostgresPure/scenario.md), on SQL Server. See that scenario's
`scenario.md` for the full topology/mechanism writeup — this file only notes what differs.

## What differs from PostgresPure

- `RepoType`/`RepositoryType`: `SqlServer`.
- Connection strings carry `TrustServerCertificate=True;Max Pool Size=300;`.
- RCSI (`READ_COMMITTED_SNAPSHOT`) is enabled automatically by `SqlServerDatabaseProvisioner` for
  every SqlServer database this scenario's JSON declares — no extra JSON/code needed.
- No other logic differs — `MigratingModeTestPhase1EmulatorBase` is shared verbatim across every
  repo-type variant of this scenario.
