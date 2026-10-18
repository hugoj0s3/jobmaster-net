# ClusterIdTest — PostgresPure

Checks that a job or static schedule lands on the right cluster when its cluster id comes from the handler, definition or profile instead of being passed to the call.

## Setup

One `pg-cluster-id` container runs `TargetTestClusterIdApp` with two standalone Postgres clusters:
- `cluster-id-default` (default)
- `cluster-id-target`

The `api` container sees both clusters.

`TargetTestClusterIdApp` is a separate app, not the shared schedule or recurring app. A cluster id on a handler, definition or profile that isn't configured fails startup validation. If these handlers lived in a shared app, every other scenario using that app would fail to start.

## Phase 1

Nothing below passes a `clusterId`.

| Path | How the cluster is set | Expected cluster |
|:---|:---|:---|
| One-off, `attribute` | `[JobMasterClusterId]` on the handler | `cluster-id-target` |
| One-off, `advanced` | `Advanced.OnceNowAsync<TDefinition>()`, `Config.ClusterId` | `cluster-id-target` |
| One-off, `default` (control) | none | `cluster-id-default` |
| Static recurring, attribute | `[JobMasterClusterId]` + `[TimeSpanIntervalSchedule]`, no profile | `cluster-id-target` |
| Static recurring, profile | profile `ClusterId` on a plain handler | `cluster-id-target` |

Each path is checked in two ways:
- **Where the job ran:** the handler records `JobContext.ClusterId` to Redis, and every recorded execution must show the expected cluster.
- **Where the API finds it:** each one-off job must be stored on the expected cluster, with none on the other cluster. Each static schedule must be registered on the target cluster and not on the default cluster.

The control path proves the assertions can tell the two clusters apart.

**Verified 2026-10-07:**
- Passes (about 7 minutes, including image builds).
- Fails as expected when `JobUtil.GetClusterId` is mutated to ignore the attribute and definition.
