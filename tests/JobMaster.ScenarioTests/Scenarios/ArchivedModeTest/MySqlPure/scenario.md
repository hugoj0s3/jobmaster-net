# ArchivedModeTest.MySqlPure

Same as [`PostgresPure`](../PostgresPure/scenario.md), backed by MySql instead of Postgres. See that
scenario's `scenario.md` for the full topology/mechanism writeup — this file only notes what differs.

## What differs from PostgresPure

- `RepoType`/`RepositoryType`: `MySql` instead of `Postgres` throughout.
- Connection strings carry the two MySql-provider-mandatory flags: `UseAffectedRows=True;AllowUserVariables=True`
  (required — JobMaster's MySQL provider relies on affected-row counts to detect lock races/partial
  updates).
- Container name `mysql-archive` (vs. `pg-archive`); database names unchanged (`ArchiveSource`,
  `ArchiveSourceAgent`, `ArchiveTarget`).
- No other logic differs — `ArchivedModeTestPhase1EmulatorBase` is shared verbatim across every
  repo-type variant of this scenario.
