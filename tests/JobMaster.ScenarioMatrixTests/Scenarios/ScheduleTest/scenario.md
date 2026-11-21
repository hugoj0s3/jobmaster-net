# ScheduleTest

Scheduling end to end on three clusters, then the lifecycle around it: data retention purge, a real drain to zero
buckets next to an unchanged control cluster, and retiring the drained connections. Matrix port of
`JobMaster.ScenarioTests/Scenarios/ScheduleTest` (6 per-provider copies there). Around 1 h per flavor.

## Topology
One container per cluster, each the default cluster of its own container:
- `mx-<flavor>-standalone`: standalone (no agent connection), one worker.
- `mx-<flavor>-dist-one` and `mx-<flavor>-dist-two`: distributed, 3 agent connections each (`agent-1/2/3`, the
  flavor's agent provider; dist-one uses agent slots 1–3, dist-two slots 4–6, so they never share agent databases),
  one worker per connection. `TransientThreshold` 2 min.

The standalone cluster runs on every flavor (on PostgresNats and HybridNatsRavenDb it's the same as on their master's
Pure flavor; it runs alongside the other two, so it costs no extra time). The regular suite's NATS and hybrid
variants left it out.

## Phases
- **Phase 1:** about 1000 jobs, scheduled up front: per cluster 150 `fast`, 50 `normal`, 3 `slow`, 20
  `advanced-fast` now, and 100 `fast`, 30 `normal`, 10 `advanced-fast` in 5 min. One priority per handler type.
  - No delayed batch runs before its 5 min delay (checked right after scheduling and again after the immediate work),
    even though it's threshold-eligible after 2 min; each runs once due, not more than 10 s early.
  - Every batch runs exactly once; through the API, every cluster's jobs are `Succeeded` and match the plan exactly,
    filtered by cluster, by cluster + job definition, by cluster + priority, and by cluster + test identifier.
- **Phase 2:** the standalone cluster gets `DataRetentionTtl` 10 min (the floor) and its jobs are purged (20 min
  max). dist-one becomes a coordinator plus its 3 workers in Drain mode (a coordinator is needed to hand the old
  workers' Lost buckets to the drainers); its jobs stay, and every bucket is destroyed by the real drain (30 min max).
  dist-two restarts unchanged and keeps every job: the TTL causes the purge, not the restart.
- **Phase 3:** dist-one restarts with its coordinator only (a Drain worker keeps heartbeating its connection). Its
  agent connections go dead (3 min max), then `CleanupDeadAgentConnectionsRunner` deletes them after its fixed
  30 min dead threshold (40 min max). The coordinator's reserved fallback connection is never deleted.

## Flavors
All six.
