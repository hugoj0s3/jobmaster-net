# StandaloneToDistributedTest.SqlServerPure

Same as [`PostgresPure`](../PostgresPure/scenario.md), backed by SqlServer instead of Postgres. See
that scenario's `scenario.md` for the full topology/mechanism/bug-history writeup — this file only
notes what differs.

## What differs from PostgresPure

- `RepoType`/`RepositoryType`: `SqlServer` instead of `Postgres` throughout.
- Connection strings carry the SqlServer-provider-mandatory flags: `TrustServerCertificate=True;Max Pool Size=300;`.
  RCSI (`READ_COMMITTED_SNAPSHOT`) is applied automatically by `SqlServerDatabaseProvisioner` for every
  database this JSON declares — no extra config needed.
- Container names `sqlserver-standalone`/`sqlserver-coordinator`/`sqlserver-executor` (vs. `pg-*`);
  agent connection `sqlserver-agent-dist` (vs. `pg-agent-dist`); database names unchanged.
- No other logic differs — `StandaloneToDistributedTestPhase1EmulatorBase`/
  `StandaloneToDistributedTestPhase2EmulatorBase` are shared verbatim across every repo-type variant
  of this scenario.
