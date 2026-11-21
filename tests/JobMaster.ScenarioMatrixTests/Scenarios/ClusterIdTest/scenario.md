# ClusterIdTest

Checks that a job or static schedule lands on the right cluster when its cluster id comes from the handler,
definition or profile instead of being passed to the call. Matrix port of
`JobMaster.ScenarioTests/Scenarios/ClusterIdTest` (Postgres only there).

## Setup
One `mx-<flavor>-cluster-id` container runs `TargetTestClusterIdApp` with two standalone clusters:
- `mx-<flavor>-cid-default` (default)
- `cluster-id-target`: fixed name on every flavor, because the app names it in attributes (`ClusterIds.Target` is a
  compile-time constant). Each flavor still has its own databases, so the shared name never collides.

The `api` container sees both clusters.

`TargetTestClusterIdApp` is a separate app, not the shared schedule or recurring app: a cluster id on a handler,
definition or profile that isn't configured fails startup validation, so these handlers in a shared app would stop
every other scenario using it from starting.

## Phase 1
Nothing below passes a `clusterId`.

| Path | How the cluster is set | Expected cluster |
|:---|:---|:---|
| One-off, `attribute` | `[JobMasterClusterId]` on the handler | `cluster-id-target` |
| One-off, `advanced` | `Advanced.OnceNowAsync<TDefinition>()`, `Config.ClusterId` | `cluster-id-target` |
| One-off, `default` (control) | none | `mx-<flavor>-cid-default` |
| Static recurring, attribute | `[JobMasterClusterId]` + `[TimeSpanIntervalSchedule]`, no profile | `cluster-id-target` |
| Static recurring, profile | profile `ClusterId` on a plain handler | `cluster-id-target` |

Each path is checked two ways:
- **Where the job ran:** the handler records `JobContext.ClusterId` to Redis; every recorded execution must show the
  expected cluster.
- **Where the API finds it:** each one-off job is stored on the expected cluster and none on the other; each static
  schedule is registered on the target cluster and not on the default one.

The control path proves the assertions can tell the two clusters apart.

## Flavors
PostgresPure, MySqlPure, SqlServerPure, RavenDbPure. PostgresNats and HybridNatsRavenDb are excluded: both clusters
are standalone, so they'd run the same as PostgresPure and RavenDbPure.
