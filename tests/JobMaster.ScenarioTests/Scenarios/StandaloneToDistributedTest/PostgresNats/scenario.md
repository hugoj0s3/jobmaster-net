# StandaloneToDistributedTest.PostgresNats

Same as [`PostgresPure`](../PostgresPure/scenario.md), with Phase2's Execution worker backed by NATS
JetStream instead of Postgres. See that scenario's `scenario.md` for the full
topology/mechanism/bug-history writeup — this file only notes what differs.

## What differs from PostgresPure

- Master storage stays Postgres throughout — NATS can only ever be an agent (transport) connection,
  never a cluster's master repository (`NatsJetStreamConnectionOptionsStrategy.SetOptions` throws if
  attempted).
- **Phase1 (`postgres-nats-standalone`) is unchanged apart from naming** — a `Standalone: true`
  worker has no `AgentConnections` at all (it uses the reserved standalone connection, itself always
  the same physical connection as master), so there is nothing for NATS to replace there.
- **Phase2's executor's agent connection** (`pg-agent-dist` in `PostgresPure`) becomes
  `nats-agent-dist`, `RepositoryType: NatsJetStream`, connection string
  `nats://{{NatsUsername}}:{{NatsPassword}}@{{NatsHost}}:{{NatsPort}}`.
- **`TransientThreshold` is explicit at `00:02:00` on *both* `postgres-nats-coordinator` and
  `postgres-nats-executor`'s cluster config templates** — not just the one with the NATS agent
  connection. Both processes independently upsert the same `ClusterId`'s persisted config at
  startup; setting it identically on both sides avoids relying on assumptions about which process's
  config write "wins" for the shared value. Any cluster with a NATS agent connection must keep
  `TransientThreshold` at or under `NatsJetStreamConstants.MaxThreshold` (5 minutes) or app startup
  throws — unlike a SQL-only cluster, this can't be left to the 10-minute default.
- No other logic differs — `StandaloneToDistributedTestPhase1EmulatorBase`/
  `StandaloneToDistributedTestPhase2EmulatorBase` are shared verbatim across every repo-type variant
  of this scenario.
