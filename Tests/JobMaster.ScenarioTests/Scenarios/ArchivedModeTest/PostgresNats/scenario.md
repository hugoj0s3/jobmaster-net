# ArchivedModeTest.PostgresNats

Same as [`PostgresPure`](../PostgresPure/scenario.md), with `archive-source`'s agent connection
backed by NATS JetStream instead of Postgres. See that scenario's `scenario.md` for the full
topology/mechanism writeup — this file only notes what differs.

## What differs from PostgresPure

- Master storage for both clusters stays Postgres — NATS can only ever be an agent (transport)
  connection, never a cluster's master repository (`NatsJetStreamConnectionOptionsStrategy.SetOptions`
  throws if attempted).
- `archive-source`'s single agent connection (`pg-agent-source` in `PostgresPure`) becomes
  `nats-agent-source`, `RepositoryType: NatsJetStream`, connection string
  `nats://{{NatsUsername}}:{{NatsPassword}}@{{NatsHost}}:{{NatsPort}}`.
- `archive-source.TransientThreshold` is explicit at `00:02:00` — any cluster with a NATS agent
  connection must keep `TransientThreshold` at or under `NatsJetStreamConstants.MaxThreshold` (5
  minutes) or app startup throws; unlike a SQL-only cluster, this can't be left to the 10-minute
  default.
- `archive-target` is unchanged — `ClusterMode.Archived` clusters run Coordinator only, no agent
  connections at all (`PreValidation` forbids them), so there is nothing for NATS to replace there.
- No other logic differs — `ArchivedModeTestPhase1EmulatorBase` is shared verbatim across every
  repo-type variant of this scenario.

## Worth watching on first run

The recurring schedule's 6-minute interval is close to the lowered 5-minute `TransientThreshold`
ceiling — closer than `MigratingModeTest.PostgresNats`'s 15-second interval, and in the same
territory that made `RecurringScheduleTest.PostgresNats` need `WaitForTwoFiringsTimeout`/
`FirstFiringLateTolerance` overrides. This scenario cancels the schedule immediately after creating
it though (it never lets `RecurringSchedulePlanner` actually plan an occurrence), so those overrides
may not be needed here — calibrate from the actual first run rather than assuming, per this
scenario's own convention of computing timeouts empirically.
