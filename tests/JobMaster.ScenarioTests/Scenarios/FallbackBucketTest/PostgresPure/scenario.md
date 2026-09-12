# FallbackBucketTest.PostgresPure

Tests `AssignJobsToBucketsRunner`'s fallback-bucket mechanism end to end: a cluster with a
Coordinator and *no* agent workers at all still gets its jobs processed, instead of starving them
forever. Single phase.

## Topology

**One container, Coordinator only, no `AgentConnections` block at all** — this is what guarantees
`GetBucketAvailableForJobAsync` can never find a real bucket for any job, no matter how long it
waits.

| Cluster | Mode | Workers | Notes |
|---|---|---|---|
| `fallback-bucket` | `Active` (default) | Coordinator only | No agent connections in the config at all — not just none currently alive, none declared. |

## Flow

1. Schedule 30 `fast` jobs against `fallback-bucket`.
2. Wait for all 30 to reach `Succeeded` (Redis-tracked executions + API-persisted `Succeeded` jobs,
   cross-checked exactly).

Underneath, `AssignJobsToBucketsRunner.HandleJobFallbackAssignmentAsync` tracks, per
`(ClusterId, WorkerLane, Priority)`, the first time no real bucket could be found for a job. Once
`JobMasterConstants.NoBucketFallbackThreshold` (2.5 minutes, a fixed framework constant, not
configurable per cluster) elapses with still no real bucket, it creates a `BucketType.Fallback`
bucket backed by a reserved, master-database-backed connection
(`JobMasterConstants.MasterFallbackAgentConnName`) and starts an inline
`PollingJobsExecutionRunner` **on the Coordinator process itself** to service it — no real agent
worker is ever involved.

3. Assert exactly one bucket exists with `BucketType == Fallback` (2) — proves the mechanism
   actually activated, not just that jobs eventually succeeded through some other path.

## What this doesn't cover (deliberately)

- **Fallback bucket lifecycle after use** — a lost `Fallback`-type bucket is sent straight to
  `ReadyToDelete` rather than through the normal connection-matching drain path
  (`AssignedLostBucketsRunner`), since no real worker can ever own the reserved fallback
  connection. Not exercised here (this scenario never kills the Coordinator).
- **Priority/lane misconfiguration variety** — the scenario only exercises the simplest trigger
  (zero agent connections declared at all). `ResolveFallbackPriority`'s preference-order fallback
  (Critical → High → Medium, skipping any priority the cluster has disabled) isn't separately
  exercised.
- **No `PostgresNats` variant** — this scenario's whole point is a cluster with *zero* agent
  connections; there's nothing for a NATS-backed agent connection to replace, so a `PostgresNats`
  variant would be byte-identical to this one. Skipped deliberately, not an oversight.

## Timing

`NoBucketFallbackThreshold` (2.5 min) is a fixed framework constant. First real run took 6m39s
end to end (activation wait + onboarding + 30 fast-handler executions), well under the 10-minute
`ExecuteWaitTimeout`.
