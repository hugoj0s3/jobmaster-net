# StandaloneToDistributedTest.MySqlPure

Same as [`PostgresPure`](../PostgresPure/scenario.md), backed by MySql instead of Postgres. See
that scenario's `scenario.md` for the full topology/mechanism/bug-history writeup — this file only
notes what differs.

## What differs from PostgresPure

- `RepoType`/`RepositoryType`: `MySql` instead of `Postgres` throughout.
- Connection strings carry the two MySql-provider-mandatory flags: `UseAffectedRows=True;AllowUserVariables=True`
  (required — JobMaster's MySQL provider relies on affected-row counts to detect lock races/partial
  updates).
- Container names `mysql-standalone`/`mysql-coordinator`/`mysql-executor` (vs. `pg-*`); agent
  connection `mysql-agent-dist` (vs. `pg-agent-dist`); database names unchanged.
- No other logic differs — `StandaloneToDistributedTestPhase1EmulatorBase`/
  `StandaloneToDistributedTestPhase2EmulatorBase` are shared verbatim across every repo-type variant
  of this scenario.
