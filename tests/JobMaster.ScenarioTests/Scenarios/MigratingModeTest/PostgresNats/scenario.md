# MigratingModeTest.PostgresNats

Same as [`PostgresPure`](../PostgresPure/scenario.md), with `migrating-target`'s agent connection
backed by NATS JetStream instead of Postgres. See that scenario's `scenario.md` for the full
topology/mechanism writeup — this file only notes what differs.

## What differs from PostgresPure

- Master storage for both clusters stays Postgres — NATS can only ever be an agent (transport)
  connection, never a cluster's master repository (`NatsJetStreamConnectionOptionsStrategy.SetOptions`
  throws if attempted).
- `migrating-source` is unchanged: `Mode: Migrating`, Coordinator only, no agent connections at
  all — nothing for NATS to replace there.
- `migrating-target`'s single agent connection (`pg-agent-target` in `PostgresPure`) becomes
  `nats-agent-target`, `RepositoryType: NatsJetStream`, connection string
  `nats://{{NatsUsername}}:{{NatsPassword}}@{{NatsHost}}:{{NatsPort}}`.
- `migrating-target.TransientThreshold` is explicit at `00:02:00` — any cluster with a NATS agent
  connection must keep `TransientThreshold` at or under `NatsJetStreamConstants.MaxThreshold` (5
  minutes) or app startup throws; unlike a SQL-only cluster, this can't be left to the 10-minute
  default.
- No other logic differs — `MigratingModeTestPhase1EmulatorBase` is shared verbatim across every
  repo-type variant of this scenario.

## Worth watching on first run

The recurring schedule's 15-second interval is far below even the lowered 5-minute
`TransientThreshold` ceiling, so this scenario is *not* expected to need the kind of
`WaitForTwoFiringsTimeout`/`FirstFiringLateTolerance` override `RecurringScheduleTest.PostgresNats`
needed (that divergence only shows up when the recurring interval is close to or exceeds the
threshold). Calibrate from the actual first run rather than assuming, per this scenario's own
convention of computing timeouts empirically.
