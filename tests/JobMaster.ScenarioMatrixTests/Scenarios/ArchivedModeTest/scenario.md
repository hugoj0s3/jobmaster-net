# ArchivedModeTest

The archived-mode safety net: work written to a cluster right before it became Archived (the scheduler's mode
check isn't atomic with the write) isn't stranded: once the cluster runs Archived with a `TargetActiveClusterId`, its
coordinator moves the late jobs and schedules there, same ids, none lost. Replaces phases 2–3 of
the earlier per-provider suite's `ArchivedModeTest` (PostgresPure only there); its phase 1 (data-retention
archiving) is the matrix `DataRetentionArchiveTest`.

## Topology
One container `mx-<flavor>-stray`, two clusters:
- `mx-<flavor>-stray-old`: phase 1 `Mode: Migrating` (to stray-new) with **no workers**, so nothing moves the work
  written there; phase 2 `Mode: Archived`, `TargetActiveClusterId` stray-new, a coordinator.
- `mx-<flavor>-stray-new`: phase 1 no workers; phase 2 an Active cluster with one agent connection (`agent-1`, the
  flavor's agent provider), a coordinator and an execution worker. `TransientThreshold` 2 min (NATS caps it at 5).

## Phases
- **Phase 1:** schedule 20 `fast` jobs and a `00:00:15` recurring schedule on stray-old; both are on it. Stop the
  container, then switch stray-old's **stored** `ClusterMode` to Archived directly in the master database
  (`ScenarioFlavor.SetStoredClusterModeAsync`): the switch landed right after those writes passed the mode check.
- **Phase 2:** stray-old restarts as Archived while still holding the late work. Startup must not refuse it (with a
  target, the unfinished-data check is skipped). Its coordinator moves the jobs and the schedule to stray-new
  (7 min max each); all 20 run exactly once there and are `Succeeded` with their ids; the schedule is on stray-new
  with its id, `Active`, and fires there (10 min max), then it's cancelled.

## The raw mode switch
The stored mode is the cluster's `ClusterConfiguration` generic record (entry id = cluster id), value `ClusterMode`:
- SQL (Postgres, MySQL, SQL Server): `jm_generic_record_entry_value.value_int64` joined to `jm_generic_record_entry`;
  only the UPDATE-with-join syntax differs.
- RavenDB: document `JM_ClusterConfiguration/<clusterId>/<clusterId>`, `Values.ClusterMode`, patched by id.

## Flavors
All six: the switch is per master provider (PostgresNats uses the Postgres one, HybridNatsRavenDb the RavenDB one),
and stray-new's agent connection is the flavor's.
