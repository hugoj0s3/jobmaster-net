# PostgresNats

Proves ~1000 jobs scheduled against a Postgres-master / NATS-JetStream-transport cluster pair all
execute exactly once — no losses, no duplicate delivery — and that a job scheduled well beyond
`TransientThreshold` is not dispatched early. Mixed providers: Postgres for master/cluster storage,
NATS JetStream for the agent/transport layer (job dispatch queue), unlike the `*Pure` scenarios
which use one provider for everything.

## Topology

- No standalone cluster: `NatsJetStreamConnectionOptionsStrategy.SetOptions(IClusterConfigSelector,
  ...)` throws `"NATS JetStream is not supported as a cluster master repository"` — NATS can only
  ever be an *agent connection*, never a cluster's own master storage. So unlike `PostgresPure`
  (which pairs a standalone cluster with two distributed ones), this scenario only has distributed
  clusters.
- `postgres-nats-dist-one`, `postgres-nats-dist-two` — two `TargetTestScheduleApp` containers, each
  a distributed cluster: a Postgres master DB (`PostgresNatsDistCluster`, shared by both) plus three
  NATS JetStream agent connections (`nats-agent-1/2/3`), each with its own worker. All three agent
  connections share the same physical NATS server/connection string — unlike the SQL providers,
  where each agent connection gets its own *database*, NATS has no equivalent partitioning concept
  at the connection-string level; each agent connection gets its own JetStream stream instead,
  namespaced by agent connection id (`NatsJetStreamUtils`), so reusing one connection string across
  all three is the correct mirror of the SQL providers' three-separate-databases pattern, not a
  simplification.
- `api` — one `TargetTestApi` container, zero workers, registering both clusters (master-DB-only,
  Postgres) so job count/list queries can be cross-checked against the JobMaster API, not just
  Redis. The API never talks to NATS directly — job metadata lives in the Postgres master DB
  regardless of which provider handles dispatch.
- Every cluster's `TransientThreshold` is `00:02:00` (2 minutes) — comfortably under
  `NatsJetStreamConstants.MaxThreshold` (5 minutes), the ceiling `NatsJetStreamJobMasterRuntimeSetup`
  enforces at startup for any cluster with a NATS agent connection.
- The master database is shared across both clusters, isolated only by `ClusterId` — same pattern as
  `PostgresPure` and every other scenario in this suite. The NATS server is likewise shared across
  both clusters, isolated by JetStream stream naming instead of by database.

## Infrastructure

A `NatsContainer` (`Testcontainers.Nats`, image `nats:2.10-alpine`) is started lazily by
`ScenarioGlobalEnvironment.GetOrStartNatsAsync`, the same run-scoped-singleton pattern as the
Postgres/MySql/SqlServer/Redis containers — one NATS server for the whole test run, network alias
`nats`, port `4222`, credentials generated once per run (`ScenarioGlobalEnvironment.NatsPassword`,
never hardcoded, never logged). JetStream is enabled by default (`NatsBuilder` always passes
`--jetstream`); no extra provisioning step is needed the way `PostgresDatabaseProvisioner` etc. are
needed for the SQL providers — `NatsJetStreamJobMasterRuntimeSetup` provisions streams itself inside
the app container at startup. `ScenarioRunner.EnsureDatabasesForContainerAsync` starts the NATS
container whenever a rendered cluster config's `RepoType` or any `AgentConnections[].RepositoryType`
is `"NatsJetStream"`, same detection mechanism as the SQL providers.

`Tests/TargetTestScheduleApp` (the app image every scenario container runs) now references
`JobMaster.NatsJetStream` and force-loads its assembly at startup
(`Assembly.LoadFrom(...JobMaster.NatsJetStream.dll)`), matching the existing Postgres/MySql/SqlServer
pattern — required because `ConfigFromJson` only sets `RepositoryType` as a string, so the CLR would
never naturally load the provider assembly, and the `[JobMasterIocRegistration]` AppDomain-reflection
scan can't see types in an unloaded assembly.

## What Phase1 does

`PostgresNatsTests.RunAllPhases` (via `PostgresNatsPhase1Emulator` → the shared
`PureScheduleTestPhase1EmulatorBase`, identical logic to `PostgresPure`/`MySqlPure`/`SqlServerPure`,
just with 2 clusters instead of 3 since there's no standalone leg):

1. Builds a `List<JobsQty>` plan up front. The base per-cluster quantities (150 `fast` + 50 `normal`
   + 3 `slow` immediate, plus 100 `fast` + 30 `normal` delayed 5 minutes = 333/cluster) are
   calibrated for the original 3-cluster `*Pure` scenarios (999 total); `BuildPlan` scales them by
   `3.0 / clusterCount` so any scenario's total stays near ~1000 jobs regardless of cluster count.
   With 2 clusters here, that's 225 `fast` + 75 `normal` + 5 `slow` immediate, plus 150 `fast` + 45
   `normal` delayed = 500/cluster × 2 clusters = 1000 jobs total.
2. Schedules every batch up front, recording each batch's own scheduled-at timestamp.
3. Asserts delayed batches show zero executions immediately, and again after all immediate work
   completes — proving `TransientThreshold` doesn't dispatch a 5-minute-out job early (and, for NATS
   specifically, that jobs held past their `TransientThreshold` window are correctly held on the
   master rather than onboarded into JetStream early).
4. Waits out the remaining delay, then asserts each delayed batch executes in full, no earlier than
   its due time.
5. Cross-checks everything against both the Redis tracker and the JobMaster API
   (`page size = int.MaxValue`), per-cluster and per-handler-type.

## What Phase2 does

`PostgresNatsPhase2Emulator` (via the shared `DataRetentionPhase2EmulatorBase`) reuses the ~1000
jobs Phase1 already scheduled and finished, and exercises the real drain-to-completion bucket
lifecycle against `postgres-nats-dist-one` — same mechanics as `PostgresPure`/`MySqlPure`/
`SqlServerPure`'s Phase2, but this is the only place in the whole codebase that exercises NATS's
own drain-runner implementations (`NatsJetStreamDrainSavePendingJobsRunner`,
`NatsJetStreamDrainProcessingRunner`, `NatsJetStreamDrainSavePendingRecurringScheduleRunner`)
against real bucket-status transitions. The existing IntegrationTests drain coverage
(`NatsJetStreamDrainModeTests`) boots the same runners but only asserts final job-completion
counts — it never checks `Draining`/`ReadyToDrain`/`ReadyToDelete` transitions or connection
deletion, which is exactly what this phase (and Phase3) verify.

Unlike the SQL `*Pure` scenarios, there's **no `TtlOnlyClusterId`** here —
`DataRetentionTtl`-based purge (`DeleteOldFinalJobsRunner`) is pure master-side logic that never
touches the agent/transport layer at all, so it's already fully proven by the three SQL `*Pure`
scenarios; retesting it against a NATS-backed cluster wouldn't exercise anything NATS-specific,
just add redundant runtime. `DataRetentionPhase2EmulatorBase.TtlOnlyClusterId` defaults to `null`
and is left unoverridden here for exactly that reason.

- `postgres-nats-dist-one` (`DrainClusterId`) — restarted with **four workers in one process**: a
  `Coordinator`-mode worker (no `AgentConnectionName`) plus its original 3 workers switched to
  `Drain` mode (still with their `AgentConnectionName`), identical topology to the SQL `*Pure`
  scenarios' drain cluster. `TransientThreshold` stays explicit at `00:02:00` in this phase's
  config (unlike the SQL `*Pure` scenarios' Phase2, which drop the field and rely on the 10-minute
  default) — `NatsJetStreamJobMasterRuntimeSetup.ValidateAsync` rejects startup for any cluster
  with a NATS agent connection whose `TransientThreshold` exceeds `NatsJetStreamConstants.MaxThreshold`
  (5 minutes), so the field can't be omitted here the way it can for a SQL-only cluster. Phase2
  waits for this cluster's bucket count to reach zero — proving the real drain (`Lost` →
  `ReadyToDrain` → `Draining` → `ReadyToDelete` → destroyed) actually finished through NATS's own
  drain runners, not a shortcut.
- `postgres-nats-dist-two` (`ControlClusterId`) — restarted with the *same* config Phase1 used, no
  worker-mode change. Proves the drain lifecycle on `dist-one` is what destroys its buckets, not
  merely the app-container restart every Phase2 container goes through.

## What Phase3 does

`PostgresNatsPhase3Emulator` (via the shared `DataRetentionPhase3EmulatorBase`, unchanged and
reused as-is — it has no TTL dependency, so it needed no changes to work here) only touches
`postgres-nats-dist-one` — the cluster whose buckets Phase2 already proved are fully drained and
destroyed. Its container is restarted a second time with **only the Coordinator worker** (the 3
Drain workers, and therefore their NATS agent-connection heartbeats, are gone entirely) — mirroring
the SQL `*Pure` scenarios' Phase3 exactly. Phase3 asserts the connections go dead fast
(`ApiAgentConnection.IsAlive` → `false`, ~45s) and then get physically deleted by
`CleanupDeadAgentConnectionsRunner`'s hardcoded 30-minute dead threshold plus its own 5-minute tick
— both entirely generic Sdk-level mechanics operating on the master DB's connection records,
regardless of which transport backed them.

This scenario's Phase2/Phase3 runtime is dominated by the same waits as the SQL `*Pure` scenarios
(~20-30 min drain wait, ~30-40 min connection-deletion wait); expect roughly 60-70 minutes total
for Phase1+Phase2+Phase3, same order of magnitude as `PostgresPure`.
