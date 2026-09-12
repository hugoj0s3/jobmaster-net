# MigratingModeTest.MySqlPure

Same as [`PostgresPure`](../PostgresPure/scenario.md), on MySQL. See that scenario's `scenario.md`
for the full topology/mechanism writeup — this file only notes what differs.

## What differs from PostgresPure

- `RepoType`/`RepositoryType`: `MySql`.
- Connection strings carry `UseAffectedRows=True;AllowUserVariables=True` — required by
  JobMaster's MySQL provider, which relies on affected-row counts (not matched-row counts) to
  detect lock races and partial updates.
- No other logic differs — `MigratingModeTestPhase1EmulatorBase` is shared verbatim across every
  repo-type variant of this scenario.
