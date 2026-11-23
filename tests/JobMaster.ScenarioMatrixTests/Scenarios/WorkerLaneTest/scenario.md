# WorkerLaneTest

Jobs reach the worker of their lane, on a standalone and on a distributed cluster. Added when standalone workers got a
worker lane: before, a standalone worker had none, so a lane job found no bucket and only ran through the no-bucket
fallback, about 3 min 48 s later.

## Topology
One cluster, two `TargetTestScheduleApp` containers, each with one worker:
- `mx-<flavor>-<topo>-lane-default`: a worker without a lane.
- `mx-<flavor>-<topo>-lane-billing`: a worker with lane `billing`.

`<topo>` is the topology axis:
- `std`: a **standalone** cluster (`"Standalone": true`), workers configured through `ConfigFromJson`, so the JSON
  `WorkerLane` is covered too.
- `dist`: a distributed cluster, both workers on agent connection `agent-1` (the flavor's agent provider).
  `TransientThreshold` 2 min (NATS caps it at 5).

Each container's hostname is its container name, so the tracker's `HostId` tells which worker ran a job.

## Flow
1. Schedule 10 `fast` jobs without a lane, 10 `fast` jobs with `workerLane: "billing"` on the call, and 10
   `billing-lane` jobs (lane from the handler's `[JobMasterWorkerLane("billing")]`).
2. Each job runs exactly once (Redis tracker, re-read after a 2 s settle window), within 120 s: under the no-bucket
   fallback, so a job that only ran through the fallback fails.
3. Jobs without a lane ran only on `lane-default`; both kinds of billing jobs only on `lane-billing`.

## Flavors
- Distributed: every flavor.
- Standalone: PostgresPure, MySqlPure, SqlServerPure, RavenDbPure. PostgresNats and HybridNatsRavenDb are excluded: a
  standalone cluster has no agent connection, so they'd run the same as PostgresPure and RavenDbPure.

Run one unit: `dotnet test tests/JobMaster.ScenarioMatrixTests --filter "FullyQualifiedName~WorkerLaneTest&DisplayName~MySqlPure&DisplayName~Standalone"`.
