# HybridNatsRavenDb

Same phases as `PostgresNats`, but every agent connection is a **hybrid** connection: one provider
for transport (saving newly scheduled jobs) and another for execution (dispatching due jobs to
workers, running them, draining processing buckets). Each cluster has both shapes, so every phase
runs through NATS → RavenDB and RavenDB → NATS at the same time.

## Topology

- `hybrid-dist-one`, `hybrid-dist-two`: two `TargetTestScheduleApp` containers, each a distributed
  cluster with a RavenDB master (`HybridDistCluster`, shared by both, isolated by `ClusterId`) and
  two hybrid agent connections, each with its own worker:
  - `hyb-nats-raven` (worker `w1`): transport NATS JetStream, execution RavenDB (`HybridAgentExec1`).
  - `hyb-raven-nats` (worker `w2`): transport RavenDB (`HybridAgentTransport2`), execution NATS JetStream.
- No standalone cluster: NATS can't be a master.
- `api`: one `TargetTestApi` container, zero workers, both clusters on the RavenDB master, used to
  cross-check job counts and bucket/connection state.
- `TransientThreshold` is `00:02:00` on every cluster. `hyb-raven-nats` has NATS on the execution
  side, where `NatsJetStreamJobMasterRuntimeSetup` caps it at 5 minutes.
- Configured through `ConfigFromJson` with per-connection `Transport` / `Execution` sections.
  `ScenarioRunner` starts NATS and RavenDB from those sections and provisions the RavenDB databases.

## What Phase1 does

`HybridNatsRavenDbPhase1Emulator` (shared `ScheduleTestPhase1EmulatorBase`, same as `PostgresNats`):
~1000 jobs across both clusters, immediate and 5-minute-delayed batches. Proves every job runs
exactly once through both connection shapes, and that delayed jobs aren't dispatched before their
time (held on master past `TransientThreshold`, whichever provider executes them). Cross-checked
against the Redis tracker and the API.

## What Phase2 does

`HybridNatsRavenDbPhase2Emulator` (shared `DataRetentionPhase2EmulatorBase`): `hybrid-dist-one`
restarts with a Coordinator plus `w1`/`w2` in `Drain` mode; `hybrid-dist-two` restarts unchanged as
the control. Waits until every bucket on `hybrid-dist-one` is destroyed by the real drain lifecycle.
Here each bucket's save-pending side and processing side are on different providers, so the drain
runners of both providers run on the same bucket, and `HasJobs` / `DestroyBucket` cover both stores.
No `TtlOnlyClusterId`: TTL purge is master-side only and already covered by the `*Pure` scenarios.

## What Phase3 does

`HybridNatsRavenDbPhase3Emulator` (shared `DataRetentionPhase3EmulatorBase`): `hybrid-dist-one`
restarts with only the Coordinator. Asserts both hybrid connections go dead (~45s) and are then
deleted by `CleanupDeadAgentConnectionsRunner` (30-minute threshold plus its 5-minute tick).

Expect roughly 60-70 minutes for all three phases, like `PostgresNats`.
