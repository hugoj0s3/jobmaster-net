# FallbackBucketTest.MySqlPure

Same as [`PostgresPure`](../PostgresPure/scenario.md), on MySQL. See that scenario's `scenario.md`
for the full mechanism writeup (`AssignJobsToBucketsRunner`'s fallback-bucket path) — this file
only notes what differs.

## What differs from PostgresPure

- `RepoType`/`RepositoryType`: `MySql`.
- Connection string carries `UseAffectedRows=True;AllowUserVariables=True` — required by
  JobMaster's MySQL provider, which relies on affected-row counts (not matched-row counts) to
  detect lock races and partial updates.
- No other logic differs — `FallbackBucketTestPhase1EmulatorBase` is shared verbatim across every
  repo-type variant of this scenario.

## What this doesn't cover (deliberately)

Same as `PostgresPure` — see that file. No `PostgresNats` variant either, for the same reason
(nothing for a NATS-backed agent connection to replace when the whole point is zero agent
connections).
