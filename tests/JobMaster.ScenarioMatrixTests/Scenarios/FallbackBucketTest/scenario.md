# FallbackBucketTest

`AssignJobsToBucketsRunner`'s fallback bucket end to end: a cluster with a coordinator and no agent workers at all
still runs its jobs instead of starving them. Matrix port of `JobMaster.ScenarioTests/Scenarios/FallbackBucketTest`
(4 per-provider copies there). Single phase.

## Topology
One container `mx-<flavor>-fallback`, coordinator only, **no `AgentConnections` declared** (not just none alive):
`GetBucketAvailableForJobAsync` can never find a real bucket, however long it waits.

## Flow
1. Schedule 30 `fast` jobs.
2. After `JobMasterConstants.NoBucketFallbackThreshold` (2.5 min, fixed), the coordinator creates a
   `BucketType.Fallback` bucket on a reserved, master-backed connection and runs it with its own
   `PollingJobsExecutionRunner` (20 min max wait).
3. Every job ran exactly once (Redis tracker), all 30 are `Succeeded` on master with their original ids, and the
   cluster has exactly one fallback bucket.

## Flavors
PostgresPure, MySqlPure, SqlServerPure, RavenDbPure. PostgresNats and HybridNatsRavenDb are excluded: no agent
connection is declared, so they'd run the same as PostgresPure and RavenDbPure.
