# ArchivedModeTest.SqlServerPure

Same as [`PostgresPure`](../PostgresPure/scenario.md), backed by SqlServer instead of Postgres. See
that scenario's `scenario.md` for the full topology/mechanism writeup — this file only notes what
differs.

## What differs from PostgresPure

- `RepoType`/`RepositoryType`: `SqlServer` instead of `Postgres` throughout.
- Connection strings carry the SqlServer-provider-mandatory flags: `TrustServerCertificate=True;Max Pool Size=300;`.
  RCSI (`READ_COMMITTED_SNAPSHOT`) is applied automatically by `SqlServerDatabaseProvisioner` for every
  database this JSON declares — no extra config needed.
- Container name `sqlserver-archive` (vs. `pg-archive`); database names unchanged (`ArchiveSource`,
  `ArchiveSourceAgent`, `ArchiveTarget`).
- No other logic differs — `ArchivedModeTestPhase1EmulatorBase` is shared verbatim across every
  repo-type variant of this scenario.
