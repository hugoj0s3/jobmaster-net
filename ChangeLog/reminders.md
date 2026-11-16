# Reminders

Follow-ups noted during work but deliberately deferred out of the PR they came up in. Grouped by kind: bugs / gaps
(something is wrong or missing today), design follow-ups (works, but should change), ideas (not designed), and
docs / benchmarks / positioning.

---

# Bugs / gaps

## Agent pull deletes before save: a stop or kill loses the batch in flight (SQL, RavenDB)

Found 2026-11-15 with `DrainModeTest` (PostgresDist / MySqlDist: master + agent = 4646 of 5000 right after phase 1's
`docker stop`; 500 = 2 × `BucketBufferSize` pulled by one executor, 146 saved, 354 lost, no log). Existing since 0.0.11: confirmed 2026-11-15 on `v0.0.11-alpha.3`, 1 of 3 runs lost 105
jobs (master 323 + agent 4572 = 4895 right after the stop, then stuck).
`PullSavePendingJobsAsync` / `PullSavePendingRecurAsync` (and the processing pull) delete the rows they return, so a
pulled batch lives only in the worker's memory until saved. Nothing reacts to process exit today (no runner stops, the
save batch's `batchTimeoutCts` isn't linked to the runner token), so even a graceful stop loses it. A first fix
(process-exit hook stopping the pull-and-save runners + put-back) worked 3/3 but was reverted 2026-11-15 to redo it as
part of the runner cleanup (stop every worker via `StopImmediatelyAsync` / `OnStopAsync`). (The
`ForEachAsyncLegacy` cancellation bug found then is fixed again, 2026-11-16, see ChangeLog.internal.)
SIGKILL, OOM or a crash would still lose it even after that. Fix (Hugo: nice change, needs a bit of work): lease instead of delete -- mark
rows leased until a time, return them, delete only after the save; expired leases are pulled again (at-least-once, as
NATS ack/nak already gives). Needs: lease columns / RavenDB metadata, a delete-by-ids after save in the runners, and
the duplicate save of a re-pulled row handled as already saved (the master insert paths already treat a duplicate id
as "exists").

Design discussed 2026-11-15 (Hugo):
- Status on the message table: `status` (`Pending` default, so existing rows stay valid / `InProgress`),
  `leased_until`, `leased_by` (worker id); index on bucket + status + lease. Provisioning adds them; existing installs
  need a migration script (`misc/migrations`). Lease > longest batch (save batch times out at 2.5 min) → ~5 min.
- Repository contract (structural seam, Hugo): `Pull(bucket, n)`, `Ack(bucket, ids)` = saved / done,
  `Release(bucket, messages)` = not saved / available again; same on the dispatcher service for jobs and recurring.
  Introduce it first with today's semantics (Ack = no-op, Release = re-insert, so `Release` takes id + payload and the
  lease version ignores the payload), then switch the implementation: Pull = lock + `InProgress` + lease (same
  SKIP LOCKED / OUTPUT / RETURNING shapes), Ack = `DELETE` by id, Release = status back to `Pending`. NATS: ack / nak.
  Runners only use the contract, so the lease is a repository-only change and the re-insert code goes away
  (`pendingTracker`, cancel-catch re-queue, `RequeueSavePendingAsync`, drain safe-guard re-enqueue).
- Auto-release (Hugo): both (1) the pull also takes `InProgress` rows whose lease expired (correct even with no
  coordinator; the drainer uses the same pull), and (2) dead-worker detection (`MarkBucketAsLostRunner`) releases the
  leases of a dead `leased_by` right away (faster than the lease, keeps the status accurate for a dashboard count).
- Execution pull deletes on pull too; a re-pulled job there means running it again, so it needs a guard (skip jobs
  already final on master) before applying the lease to it.
- RavenDB has no SKIP LOCKED: status as a document field + patch with optimistic concurrency; check its current pull
  first.
- Order (Hugo): first the runner refactor (stop on process exit via `StopAsync` / `OnStopAsync` for all runners in
  parallel; Ack/Release contract with today's semantics; one shared base for the 4 pull-and-save runners), then this.

## `RecurringScheduleTest.HybridNatsRavenDb`: two `static-cronos` runs 73 s apart (once, under load)

2026-11-15, first EC2 run (`m5.xlarge`, peak load 7.8 on 4 vCPU): `static-cronos` (`*/6 * * * *`) had 2 executions
73 s apart (expected ~360 s), different job ids. Passed twice since (`m5.2xlarge`, and locally). Undecided between: the
first `:x6` slot ran ~4–5 min late (startup under load, timing only), or two jobs were planned for the same slot (real
duplicate-planning bug). The spacing check now prints each job's `ScheduledAt` / `ExecutedAt` on failure (same
`ScheduledAt` twice = duplicate planning). To settle it: repeat the unit several times on `m5.xlarge` (needs a repeat
option in `misc/scenario-cloud/run-scenarios.sh`).
Hugo: a late first run would be a problem (a slot missed after start / deploy); extra runs right after a cancel are
fine (already-planned burst). Test gap: Cronos / NCrontab skip the first-firing check (`ChecksFirstFiringTiming:
false`, since `CreatedAt + Interval` is wrong for grid-aligned cron); re-enable it with the right expectation (first
`:x6` mark after `CreatedAt`, same 1 min tolerance) so a slow first start fails clearly.
Fix when porting the scenario to the flavor matrix (Hugo): per execution, check (1) the job's `ScheduledAt` is a real
occurrence of the expression (grid marks for cron, `CreatedAt + n × interval` otherwise), (2) `ExecutedAt − ScheduledAt`
within a tolerance, (3) each `ScheduledAt` only once, (4) count over the window ≈ occurrences in it; the average-gap
check then goes. Today it only checks count ≥ 2, unique job ids, average gap and (non-cron) first-run timing.

## `OpenApiJsonConfigSeeder` trusts the request's `Host` header for a server-side fetch (SSRF)

Bare `FromOpenApiJson()` with no explicit `ApiUrl` builds its self-fetch URL from the client-controlled
`Host` header (`src\JobMaster.Dashboard\OpenApi\OpenApiJsonConfigSeeder.cs`), and `ApplyApiUrl` can then
set `ApiUrl` — served to every browser as `ApiBaseUrl`, i.e. where login requests get sent — from that
fetch's response. A forged `Host` can redirect where every visitor's credentials go, until process restart.
`AllowedHosts`, reverse proxies, and load balancers don't reliably close this — can't rely on infra alone.
Sandbox demo is unaffected (`UseApiUrl("/jm-api")` explicitly, never calls `FromOpenApiJson()`).

Fix direction (not implemented): require `ApiUrl` explicitly; drop the Host-based self-fetch branch in
`SeedAsync`; `FromOpenApiJson(...)` keeps working only via an explicit absolute URL or file path.

## No stop entry point: workers are never stopped on host shutdown

There is no public `StopJobMasterRuntimeAsync()`, no `IHostedService`, and `JobMasterRuntime.Stop()` has no caller, so
workers are never stopped when the host shuts down; the process just ends. Only the logger flushes on exit
(`AppDomain.ProcessExit`, 2026-11-14). The graceful stop sequence itself is also unreliable, and the hard-stop path
still behaves awkwardly. Design item: a real stop path (graceful worker stop, then flush).

## Missing handler fallout (`RecurringSchedulePlanner.ScheduleNextJobsAsync`)

Current guard just logs (Critical) and skips when a handler is gone; add logic to automatically terminate the
recurring schedule after X consecutive errors or a time threshold so logs don't spam forever.

## Running the full `IntegrationTests` project unfiltered throws `Cluster ID 'RT-Postgres-1' already exists`

Raised 2026-08-21 while running the complete test suite (Unit + Integration + Scenario, no filters) ahead
of the RavenDB provider's first release. Not a RavenDB issue -- 0 of the 390 failures were RavenDB tests
(RavenDB's own conformance suite passed 142/142, reproduced on a clean re-run). Every failure was in the
Postgres/MySql/SqlServer RepoConformance suite. Hit again 2026-11-14 running a filter that matched both SQL and
RavenDB conformance classes; running them separately works.

**Root cause found 2026-08-21, confirmed from an actual CI log, not just reproduced locally.** The
triggering error is `Connection refused` trying to reach RavenDB's Testcontainers instance while resolving
`JM_ClusterConfiguration/RT-RavenDb-1/RT-RavenDb-1` -- but RavenDB's own conformance collection had *already
finished and disposed its container* by that point (RavenDB's ~40 tests all pass within the first ~30-45s).
Something in the SQL fixtures' startup path is reaching for RavenDB's already-dead container.

The mechanism: `JobMasterRuntime.StartAsync()` (`JobMaster/Sdk/Background/JobMasterRuntime.cs:78`) validates
clusters via `JobMasterClusterConnectionConfig.GetAllConfigs()` -- a **process-wide static list**, not
scoped to the `IServiceProvider` actually being started. RavenDB's `RavenDbRepositoryFixture` and SQL's
`RepoConformanceBootstrap` each register their own clusters into this same static registry, from two xUnit
collections (`RepoConformance` and `RavenDbRepoConformance`) that run *concurrently* in the same process
(both are separately marked `DisableParallelization = true` on their own `[CollectionDefinition]`, which
only affects parallelization *within* that collection, not across different collections -- confirmed
empirically, RavenDB's tests visibly interleave with SQL's container startup in the CI log). When SQL's
`StartJobMasterRuntimeAsync()` runs, it iterates *every* cluster ever registered in the process, including
RavenDB's -- which isn't part of SQL's `ServiceProvider` at all. If RavenDB's container is already torn
down by then, that step throws, so `StartJobMasterRuntimeAsync()` fails. Since `RepoConformanceBootstrap.
EnsureStartedAsync()` only sets `started = true` *after* that call succeeds (and `JobMasterClusterConnectionConfig.Create`
adds each cluster to the static list synchronously, immediately, with no rollback on a later failure), the
three SQL clusters are left registered but `started` stays `false`. A different SQL fixture then retries
`EnsureStartedAsync()`, sees `started == false`, re-enters the whole setup block, and hits "Cluster ID
'RT-Postgres-1' already exists" on the clusters the failed first attempt left behind. Reproduced
deterministically (390 failed/142 passed) both locally and in CI -- not a flake, a real race.

Two fix directions, neither implemented yet:
1. **Core-SDK fix**: scope `JobMasterRuntime.StartAsync()`'s cluster-validation loop to only the clusters
   registered on the `IServiceProvider` it was actually given, not the global static list. This is the
   architecturally correct fix, but touches core SDK production code (`JobMasterClusterConnectionConfig`'s
   registry is process-wide static by design elsewhere too), so it needs its own careful review, not a
   same-day fix.
2. **Test-infra-only fix**: merge the `RavenDbRepoConformance` and `RepoConformance` xUnit collections into
   one (e.g. give RavenDB's test classes `[Collection("RepoConformance")]` and add
   `ICollectionFixture<RavenDbRepositoryFixture>` to `RepoConformanceCollection`) so xUnit's own
   same-collection-never-parallel rule prevents the race, without touching production code. Lower risk,
   but doesn't fix the underlying static-registry scoping issue for any other future caller that hits it.

**2026-08-21**: `.github/workflows/repo-conformance-tests.yml` had its `push`/`pull_request` triggers
removed (now `workflow_dispatch`-only, matching `schedule-scenario-tests.yml`) so this stops blocking PRs
with a red badge for a test-infra race, not a real regression. Re-enable those triggers once one of the
two fixes above lands.

**2026-09-01**: Hit this exact cascade (RavenDB `ServiceUnavailable`/connection-refused + "Cluster ID
'RT-Postgres-1' already exists") repeatedly on a local dev machine while adding the archive-JobExecutions
feature, worse than usual -- one run had 426/586 tests fail. Docker on that machine had accumulated
69.67GB (92%) of reclaimable image/build-cache bloat from a day of repeated container-heavy test runs;
after `docker image prune -a` + `docker builder prune -a` reclaimed ~74GB, the very next full run (fresh
image pulls, same test code, same machine) passed clean at 574/586 (12 skipped). Not a controlled
before/after comparison (only one data point each side, and container startup timing is inherently noisy),
but consistent with disk I/O contention during RavenDB's container startup as a real contributing/amplifying
factor on top of the root cause above -- worth a Docker cleanup as a first troubleshooting step if this
cascade reappears and seems worse than the usual occasional flake.

## `TryGetApplicableIndexName` (Jobs) treats `Statuses` (plural) and `Status` (singular) inconsistently

Raised 2026-08-21 during PR review of `RavenDbMasterJobsRepository.TryGetApplicableIndexName`. The single
deployed static index (`RavenDbJobIndexes.ByClusterStatusLockNextPlanName`) maps a `Status` field, and the
method includes `StatusField` in its required-prefix tokens (making the index eligible) whenever
`criteria.Status.HasValue` -- but `criteria.Statuses.Count > 0` (the plural "in" filter) unconditionally
returns `null` instead, disqualifying the index entirely rather than also contributing a `StatusField`
token. There's no obvious RQL-level reason for the difference: `e.Status in ($statuses)` against an
indexed field is just as servable by a static index as `e.Status = $status` -- RavenDB's inverted-index
model doesn't distinguish equality from an `in` clause the way a B-tree composite key might.

Not fixed here since it wasn't verified whether this is a real gap or effectively moot: `TryGetApplicableIndexName`
is only called from `AcquireAndFetchAsync`'s candidate query and `ProbeForAcquireAsync`, and it's unclear
whether either of those two call sites is ever actually invoked with `Statuses` (plural) set in practice --
if not, this branch is defensive completeness for a `JobQueryCriteria` shape these two callers never
produce, not a real missed optimization. Worth checking actual caller usage before deciding whether to
extend the token-building logic to cover `Statuses` too.

## `TryGetApplicableIndexName` hardcodes the PartitionLockId/PartitionLockExpiresAt assumption instead of taking it as a parameter

Raised 2026-08-21 during the same PR review. `TryGetApplicableIndexName` unconditionally adds
`PartitionLockIdField`/`PartitionLockExpiresAtField` to its required-prefix tokens, justified only by a
comment ("every caller (AcquireAndFetchAsync, ProbeForAcquireAsync) only ever queries isLocked: false
candidates") -- an unenforced, comment-only invariant rather than something the method actually verifies.
`BuildWhereRql`, right next to it in the same file, already takes an explicit `bool? isLocked` parameter
for exactly this -- `TryGetApplicableIndexName` should take the same kind of explicit flag instead of
silently assuming every future caller will keep filtering on the partition lock fields. If a future call
site ever calls this method without that filter, the method would still silently build a required prefix
including those two fields, understating what the index is being asked to cover -- the kind of drift the
codebase's own "hard invariants need explicit enforcement, not just comments" convention exists to catch.

Deferred (reminder only, not fixed) per explicit request during PR review -- not touched this close to
publishing. Fix would be: give `TryGetApplicableIndexName` an `isLocked` parameter matching `BuildWhereRql`'s,
and pass it explicitly from both call sites instead of relying on the comment.

## `MasterJobsService.BulkUpdateAsync` overloads have no deadlock-specific handling

Raised 2026-08-22 (SqlServer Pure, 100k-job burst, 40 full-mode workers on one DB): 28 SQL Server deadlocks
(`SqlException 1205`), all inside `SqlServerMasterJobsRepository.BulkUpdateAsync`, all recovered by the caller's
generic retry (`WorkerClusterOperations.ExecWithRetryAsync`, coarser, not deadlock-aware). `AcquireAndFetchAsync`
returns an empty list on a deadlock (Hugo confirmed that's correct: a deadlocked claim rolls back before acquiring
anything). Still open: decide what `BulkUpdateAsync` should do on a deadlock — likely the equivalent no-op result
rather than reintroducing a retry policy.

## SQL `BulkInsertIfNotExistsAsync` still checks then inserts (not atomic)

`SqlMasterJobsRepository` / `SqlMasterRecurringSchedulesRepository.BulkInsertIfNotExistsAsync` pre-read existing ids,
then a guarded `INSERT ... WHERE NOT EXISTS`, then metadata entries for the ids the pre-read called new. The race with a
concurrent insert of the same id (duplicate key or deadlock → rollback) is handled since 2026-11-14 by a per-row
fallback, so nothing surfaces to callers. Left for later:
- Proper: atomic per-provider insert-if-absent for jobs and metadata (Postgres `ON CONFLICT DO NOTHING`, MySQL
  `INSERT IGNORE` / `ON DUPLICATE KEY`, SQL Server `MERGE`/`UPDLOCK, HOLDLOCK`), deriving "new" ids from what was
  actually inserted instead of the pre-read. Would remove the fallback's extra round trips after a race.
- `BuildQueryExistingIdsSql` (the pre-read) has no `cluster_id` filter, so a same-DB, same-prefix migrate can see the
  source cluster's row as existing and skip copying its metadata/executions/logs.

## `IanaTimeZoneId`: implicit default

Found 2026-11-14 (settings audit). The inverted startup check was fixed the same day (see ChangeLog.internal); API-only
clusters skip it via the internal `AllowOmittedTimeZone()` since the API selector can't set the zone. That skip
is a stopgap: a public `IanaTimeZoneId` on `IBaseClusterConfigSelector` would let the API follow the same rule.
Left: `ClusterConfigurationModel.IanaTimeZoneId` defaults to the local zone of whichever machine saves the cluster
configuration first, and stays (sticky). An explicit zone that overwrites a different stored one is saved without a
Warning (needs a "record exists" read: `GetFresh()` returns a default model with the local zone when nothing is stored).

Discussed fix: make it required on every cluster configuration (every process, publishers included), validated in the
aggregated startup list (missing / invalid IANA id); Warning when a process's value differs from the stored one
(changing the zone shifts recurring schedules). Breaking; reach: ~23 C# `AddJobMasterCluster` setups (tests, target
apps, fixtures, samples, benchmarks), ~189 scenario JSON files (~229 cluster entries: add `"IanaTimeZoneId": "UTC"`
next to `"RepoType"`, scripted with sed, not PowerShell), READMEs, docs.

## `DisablePriority` is applied per process, never stored

Found 2026-11-14 while auditing which cluster settings are applied from memory vs from master. Disabled priorities live
only in `JobMasterClusterConnectionConfig` (`SetDisabledPriorities` in `ClusterConfigBuilder.Finish`), never on master,
so each process applies its own: the scheduler rejects a disabled priority at the call (`JobMasterScheduler`
`EnsureCanSave`, the publisher's setting), while workers use it for bucket counts and `AssignJobsToBucketsRunner` for
the fallback priority (the worker's setting). With different configs: disabled on the publisher only → jobs rejected
though workers would run them; disabled on workers only → jobs scheduled that no bucket takes, each waits for the
fallback bucket (~2.5 min). Not visible anywhere since it's not stored.

Fix direction: store it on master like the other shared cluster settings and read it from there everywhere (scheduler
check, worker bucket counts, fallback priority); workers read it at startup, same timing as `ClusterMode`.

## NATS `TransientThreshold` limit is validated from memory but applied from master

Found 2026-11-14, same audit. `NatsJetStreamJobMasterRuntimeSetup.ValidateAsync` checks this process's
`clusterDefinition.TransientThreshold` (≤ 5 min with NATS execution), but the NATS runners use the stored value
(`NatsJetStreamRunnerBase`: `masterClusterConfigurationService.Get()?.TransientThreshold`). A NATS worker setting
2 min passes; another process of the same cluster without NATS starting later with 30 min overwrites the stored value,
and the NATS runners then run past the limit.

Fix direction: validate the value that will actually apply (this process's if set, else the stored one), and when a
process saves a larger value for a cluster with NATS execution agents, reject it (or log Critical).

## Master and agent on separate databases: covered once, not per provider

Covered: hybrid RepoConformance (Postgres master, MySQL execution leg), and the `*Nats` / hybrid scenario variants keep
agents off master. Not covered: the same setup per provider (e.g. a SQL agent on its own database of the same
provider). Same-database setups can hide gaps that only show when master and agent state are independent (e.g.
something provisioned only on "the one database"; plan item 24 was this kind of blind spot).

---

# Design follow-ups

## Graceful worker stop is not reachable

Found 2026-11-15 (worker stop cleanup). `WorkerStopCoordinatorRunner` reacts to two lock keys
(`WorkerGracefulStopLock` → `RequestStop()`, `WorkerImmediateStopLock` → `StopImmediatelyAsync()`), but nothing sets
them as a signal, and `JobMasterRuntime.Stop()` (calls `RequestStop()`) has no caller. The record-side fields
(`StopRequestedAt`, `StopGracePeriod`, `AgentWorkerStatus.Stopping`) were removed as never written; the in-memory
deadline on the worker stays (Hugo: may be good to keep). If graceful stop is finished: a public way to request it,
and exclude a stopping worker from `AssignedLostBucketsRunner`'s candidates (today a just-stopped worker's heartbeat is
fresh for ~30 s, so it can be picked as drainer of its own bucket; recovered via `MarkBucketAsLostRunner`, only
slower). Also `CleanupDeadWorkerRunner` takes `WorkerImmediateStopLock(deadWorker)` as its delete mutex, the same key
the coordinator reads as "stop now" -- give it its own key.

## `IJobMasterRuntimeSetup.OnAfterStartedAsync` — future symmetric hook

`OnStartingAsync` was renamed to `OnBeforeStartAsync` (interface + all implementers: `SqlJobMasterRuntimeSetup`,
`PostgresJobMasterRuntimeSetup`, `NatsJetStreamJobMasterRuntimeSetup`, `DefaultRuntimeValidatorSetup`) to make
room for a future `OnAfterStartedAsync` counterpart — a hook that runs once the runtime has fully started
(workers created and started, `Started = true`), symmetric to `OnBeforeStartAsync` running before any of that.
Not implemented yet; just the naming groundwork.

## RavenDB throttler and batch-size settings not calibrated under sustained load

`RavenDbJobMasterRuntimeSetup` registers master batch 250 / throttler (5, 5000 ms), agent batch 250 / internal
(5, 2500 ms) / scheduling (200, 2500 ms). The scheduling value was retuned during the 2026-09-02 throttler work (jitter
fixed RavenDB's job loss at low scheduling capacity), but unlike Postgres and MySQL there's no full calibration
sweep on record. The original 2026-08-19 comparison (burst-1000, 3 workers) only showed neither setting was the
bottleneck at that scale. Worth a dedicated pass at larger worker counts and sustained paced load; mind the Community
license cap (see Benchmarks) when comparing with the other providers.

## `AssignJobsToBucketsRunner`'s imminent-path lock caps claim throughput at one `TransferBatchSize` per 10s window

Raised 2026-08-19 while benchmarking the RavenDB provider's `AcquireAndFetchAsync` at larger burst sizes
(`JobMaster/Sdk/Background/Runners/JobAndRecurringScheduleLifeCycleControl/AssignJobsToBucketsRunner.cs`).
Core SDK behavior, not RavenDB-specific -- applies identically to every provider.

`BucketAssignerImminentLock` is a time-bucketed distributed lock keyed by a 10-second window
(`ProbeWindowInSeconds`); only the one coordinator instance that wins it does anything that tick, every
other coordinator instance just sees the lock held and skips. Combined with `TransferBatchSize` (default
1000), this means a backlog bigger than one batch needs multiple ~10-second-apart cycles to fully drain,
regardless of how many coordinator instances are configured or how fast the DB itself responds -- at
burst-5000 scale this alone plausibly accounts for a large share of both SQL Server's and RavenDB's
measured execution latency (SQL Server's ~41s Immediate mean at that scale roughly matches "4-5 cadence
cycles," suggesting its own per-cycle DB cost is small relative to the cadence gate itself).

The lock exists for a real reason, not defensively: without it, N concurrent coordinators would each
independently query the same "top TransferBatchSize unlocked, ordered by NextPlanExecutionAt asc,
Offset=0" candidate set and race to claim it. The race itself is safe (RavenDB/SQL both resolve it
correctly per-document/per-row, no double-claim), but it's wasteful -- whichever coordinators lose the
race paid for a full acquire round trip (query + patch/update + confirm) for few or zero rows, since
every concurrent query targets the identical front-of-queue window rather than different slices of the
backlog.

Three possible future directions discussed, none implemented (all deferred out of the RavenDB-scoped
PR this was raised in, and all are core-SDK changes affecting every provider):

1. **Partition candidates across concurrent coordinators** (different offsets, hash-based sharding, or
   similar) so they target genuinely different slices of the backlog instead of racing for the same
   window. Would remove the waste properly, but is a real redesign of the claim path, not a small tweak.
2. **Over-fetch and randomly thin the candidate list** -- e.g. query `TransferBatchSize * 1.5` candidates
   and randomly drop back down to `TransferBatchSize` before attempting to claim, so two coordinators
   racing at the same instant end up attempting different (though still overlapping) subsets by chance.
   Considered and set aside (2026-08-19, user's own assessment: "too odd") -- also weak on the math: for
   two independent random `k`-out-of-`N` draws, expected overlap is `k²/N` -- at `k=1000, N=1500` that's
   ~667, meaning ~67% of each coordinator's claimed candidates would still collide on average. Reduces
   wasted work, doesn't come close to eliminating it, and adds an odd "randomly discard rows you just
   fetched" step to the claim path for a small payoff.
3. **Add jitter to the coordinators' acquire-attempt cadence instead** (user's preferred direction,
   2026-08-19) -- rather than reshaping which candidates get claimed, randomize *when* each coordinator
   instance attempts the lock/probe cycle (e.g. a small random offset added to the polling interval per
   instance) so concurrent coordinators don't all hit the lock at the same instant every cycle. Doesn't
   change how many jobs get claimed per window (still capped by `TransferBatchSize`, still one winner per
   10s bucket), but should reduce simultaneous lock-contention spikes -- the wasted round trips come from
   many coordinators showing up to attempt the lock at the exact same moment, not from the lock itself.
   Lower-risk and more conventional than option 2 (standard thundering-herd mitigation, no change to
   claim/candidate semantics at all, just polling timing) -- the more promising direction to prototype
   first if this gets picked up.

## Future: a second Jobs static index will require `TryGetApplicableIndexName` to become a real index-selection mechanism

Raised 2026-08-21 (user's own forward-looking note during the same PR review, right after the two
`TryGetApplicableIndexName` items above). `RavenDbJobIndexes` currently deploys exactly one static index
(`ByClusterStatusLockNextPlanName`, mapping ClusterId/Status/PartitionLockId/PartitionLockExpiresAt/
NextPlanExecutionAt), and `TryGetApplicableIndexName`'s whole design assumes there's only ever this one
index to check against -- it unconditionally builds a required-prefix that always includes
PartitionLockId/PartitionLockExpiresAt (see the item above) and checks it via `StartsWith` against
`DeployedIndexNames` (currently a 1-element list).

The user is considering adding a second static index that does NOT map the partition-lock fields at all,
for query shapes that don't filter on lock state. Once more than one index exists, the current
"build one required-prefix string, StartsWith-match it against whatever's deployed" approach stops being
sufficient by itself -- `TryGetApplicableIndexName` needs to become genuine index *selection* among
multiple candidate indexes with different field coverage (e.g. picking the narrowest/cheapest index that
still covers a given criteria's fields), not just a yes/no check against one hardcoded shape. This is the
same underlying gap as the isLocked-parameter item above -- both stem from the method's field-inclusion
logic being hardcoded to match today's single index rather than being driven generically by what the
actual deployed index set covers. Worth designing both together whenever this gets picked up, rather than
patching the isLocked assumption first and then redoing it again once a second index shows up.

## `BulkUpdateAsync` — single SQL round-trip

The current implementation issues one SQL statement per job. Replace with a single batched statement (e.g. a `VALUES`
list joined to the target table) that atomically checks the row version, applies the update, and returns only the rows
that were actually changed. This will likely require DB-specific SQL (e.g. PostgreSQL `UPDATE … FROM (VALUES …)` or SQL
Server `MERGE`), so abstract behind the existing db-provider pattern.

## Scheduler locking strategy

Re-evaluate the locker usage (we probably no longer need the "saving" locks). Ideal design is per-resource locking;
keep the current action-level approach only if the refactor is too costly right now.

## Runner reads and scan planning

- **Cache for bucket and worker reads**: Runners currently call `QueryAllNoCacheAsync` for buckets (and workers) on
  every tick, causing redundant DB round-trips. Evaluate switching to the in-memory cache (`QueryAllAsync` or a
  dedicated cache key) so hot reads are served from memory. Invalidate on write so correctness is preserved; profile
  the cache-miss rate before deciding on TTL vs. event-driven invalidation.
- **ScanPlanner**: move LockerSlot selection into ComputeScanPlanHalfWindow result reuse across the runners.

## `DisableAutoProvisionSqlSchema` still per cluster, not per connection

The `tablePrefix` SQL setting was moved off the generic `UseSqlTablePrefixForMaster`/`UseSqlTablePrefixForAgent` methods
(removed in 0.0.12) onto each provider's own `UsePostgresForMaster(..., tablePrefix:)`/etc. parameter, because it's
genuinely per-connection. `DisableAutoProvisionSqlSchema` was deliberately left out of that pass —
`SqlJobMasterRuntimeSetup.GetAgentConfigsToProvision` reads the *cluster's own* flag even when deciding whether to
provision *agent* tables, so today it applies uniformly to the master DB and every agent connection in that cluster,
with no independent per-connection switch. Revisit if a real use case for per-connection granularity comes up (e.g.
auto-provision the master DB but manage one particular agent DB's schema manually) — at that point it could follow the
same per-provider-parameter + JSON-binder pattern `tablePrefix` now uses.

## JobDefinition-based scheduling — static recurring schedules still open

One-time and dynamic-recurring scheduling via `IJobMasterScheduler.Advanced` (`JobDefinitionConfig`/
`JobDefinitionConfigAttribute`/`IJobMasterSchedulerAdvanced`) shipped in 0.0.11-alpha — see ChangeLog.md.
Still open: `RecurringScheduleDefinitionCollection.Add<Th>()` (static recurring schedules) has the same
handler-type coupling and hasn't been given the same treatment yet.

## Clean up `JobMaster.Abstractions`

It should hold only interfaces meant for external implementation (still the intent, e.g. storage providers), even
though many of those implementations were later made internal. Over time concrete public types landed there too
(`JobDefinitionConfig`, `JobMasterDefaults`, `NeverRecursExprCompiler`, `RecurrenceCompilerFactory`, the handler
attributes in `Abstractions.Models.Attributes`, model enums, ...). Move them to namespaces that match their role, the
way `JobMaster.Serialization` now holds the concrete serializers while `IJobMasterMessageJsonSerializer` stays in
`Abstractions.Serialization`. Breaking namespace change for users: do it in one pass during alpha (ChangeLog entry, no
`[Obsolete]`), not piecemeal.

## Serialization

- **Enum store format: `SetEnumValue<TEnum>(key, value, StoreFormat = Number)`**: Today `SetEnumValue` (MsgData) is
  persisted as a number, because `InternalJobMasterSerializer` uses default STJ options. `StoreFormat` (`Number` /
  `Name`) is needed, not just nice to have (Hugo): both formats carry a versioning risk, so the user picks the one that
  fits how their enum evolves. A number breaks when members are renumbered or reordered (implicit values); a name
  breaks when a member is renamed. Only the write side changes: `TryGetEnumValue` already accepts a name or a number
  and checks it's a defined member (`[Flags]`: all bits defined), so either risk surfaces as an exception, not a wrong
  value.
- **Hypothesis: put enum support back on Metadata**: It was removed in 0.0.11 (with `byte[]`) because it "round-trips
  ambiguously". That no longer holds for reads: the read is typed, so a stored number or name is rebuilt and checked,
  as MsgData does since 0.0.12. What's left to decide is how it looks where metadata is used:
  - **Dashboard / API**: a number shows as `1`, a name as `Green`.
  - **Metadata filters**: they match the stored value, so `Color = 1` or `Color = "Green"`. Mixing formats on one key
    means a filter only matches half the data, so the format should be one per key (or per cluster), not per call.
  - **Write side**: `SetEnumValue` should come back with `StoreFormat`, same reason as MsgData. Smallest first step if
    needed: only `TryGetEnumValue` / `GetEnumValue` on `IReadableMetadata`, with users writing through
    `SetStringValue` / `SetIntValue` (as the 0.0.11 ChangeLog already advises), which already lets them choose the
    format.
  - `byte[]` stays off Metadata: useless for filtering and display, and metadata should stay small.
- **Source Generator Context (`JsonSerializerContext`)**: Upgrade `InternalJobMasterSerializer` to use
  `System.Text.Json` Source Generators. By declaring the exact types being serialized (e.g., `JobRawModel`,
  `Dictionary<string, object?>`), we can completely eliminate runtime reflection for JSON parsing, achieving AOT-level
  speed with zero allocations.

## Tests: exact-text `WithMessage` assertions

A handful of tests assert on exact or substring exception message text (e.g. `JobMasterSchedulerClusterAwareTests.cs`
asserts the full string `"Cluster mode is archived"`; several others assert `"*keyword*"` substrings). Prefer
asserting exception type (and, where the specific culprit matters, a targeted property) instead — message text is easy
to reword incidentally and breaks these tests for no functional reason. Worth a pass to check every
`WithMessage`/`.Message.Should()` usage across the suite and loosen or remove where the wording isn't the actual thing
under test.

---

# Ideas

## `JobMaster.Api` independent of the `JobMaster` package, like the Dashboard

Added 2026-11-14 (Hugo): desirable, not sure it's achievable. `JobMaster.Dashboard` has no `JobMaster` reference (only
embedded static files; it talks to the API over HTTP), so it can be deployed and versioned on its own.
`JobMaster.Api` references `JobMaster` and registers clusters through `AddJobMasterCluster` (see
`AddJobMasterClusterForApi`), so it runs the SDK's startup, validation and master services in-process, and needs the
same version as the workers plus a DB provider package per master type.
Main problem: the DB providers. The API reads the master database through the SDK's repositories, and those live in the
provider packages (`JobMaster.Postgres`, `JobMaster.MySql`, `JobMaster.SqlServer`, `JobMaster.RavenDB`), which all
depend on `JobMaster`.
Constraint (Hugo): no per-provider API packages (`JobMaster.Api.RavenDB`, `JobMaster.Api.Postgres`, ...). The API
must work with the same provider packages the workers use, or with none.
Options to look into, within that: a split of `JobMaster` into a small core (models, master repositories) and the
runtime, with the existing provider packages depending only on the core, so the API references the core plus the usual
provider package; or the API calling a worker-hosted endpoint instead of the database (no provider at all). Also: API-only quirks like the `AllowOmittedTimeZone()` exemption
exist because the API goes through the full cluster startup.

## Opt-in `EnforceJobDefinitionIdExplicit` validation setting

A handler with no explicit ID attribute silently falls back to `Type.FullName` — renaming/moving the class
then breaks resolution for anything already persisted under the old name. Idea: an opt-in setting that
fails/warns startup validation when a handler relies on that fallback, alongside the existing
duplicate-ID check in `DefaultRuntimeValidatorSetup`. Open question: handler discovery is `AppDomain`-wide,
not per-cluster, so the setting's scope (global vs. per-cluster) isn't settled.

## Cluster-aware bulk persistence

Jobs scheduled through `JobMasterSchedulerClusterAware` are written to the agent first, then asynchronously synced to
the master DB. This is intentional for performance — especially with message broker agents — but creates a durability
gap: if the agent goes down or gets corrupted before the sync completes, those jobs are lost. The risk should be very
rare or zero in healthy clusters. Plan 0.0.12 item 7 dropped an automatic flush in favor of explicit
`ConfirmOnMasterAsync`, so the gap is still open for callers that don't confirm. The goal is a durable buffering layer
that batches schedule writes and ensures they reach the master DB reliably, even under failure conditions.

**Proposed approach:**

1. **In-memory buffer** — accumulate scheduled jobs in memory inside `JobMasterSchedulerClusterAware` rather than
   dispatching each one immediately.
2. **Bulk flush** — write buffered jobs to the generic repository in a single batched operation, reducing round-trips
   and lock contention on the master DB.
3. **Partitioned records** — group jobs into fixed-size partitions (e.g., 100 jobs per record) so each write remains
   compact and independently recoverable.
4. **Dual flush triggers** — flush either when the buffer reaches the item threshold or when a configurable time window
   elapses, whichever comes first.
5. **Partition runner** — introduce a background runner that polls for unflushed partitions by group ID and promotes
   them to the master DB, decoupling write buffering from master DB availability.
6. **`SaveOperation` reuse** — once stable, evaluate whether the same mechanism can back `SaveOperation` writes.
   Requires careful analysis to avoid conflicts with the scheduler's own flush cycle.

> ⚠️ This feature introduces a write-ahead style buffer. The failure window shrinks to the flush interval rather than
> being eliminated entirely — operators should configure the flush interval based on their durability tolerance.

## Lambda adapter: reserved definition-id prefix

Not in 0.0.12 (was plan item 22, Hugo 2026-10-30). A separate project lets users schedule static-method lambdas
(Hangfire-style) to ease migration. Every job needs a definition id, so lambda jobs need ids that never collide with
user handlers.
- Per-method id with a reserved prefix, e.g. `jm-lambda:Billing.Invoices.Send(System.Guid)`: dashboard / stats show
  which method ran, and a later move to a real handler can reuse a meaningful id.
- `JobDefinitionIdResolver.GetJobHandlerTypeFromId` checks reserved prefixes first and returns the adapter handler
  type; the adapter reads method + arguments from the job data. The adapter project registers its prefix + handler
  type through a small hook (internal + `InternalsVisibleTo`, like providers, or a minimal SDK API).

Open:
- Prefix format: a character that can't be in a .NET `FullName` (e.g. `:`) so the `FullName` fallback never collides;
  consistent with `hybrid:`.
- Enforce the reservation: the startup validator rejects user `[JobMasterDefinitionId]` / `JobDefinitionConfig` ids
  starting with a reserved prefix.
- Per-job settings (priority, cluster, lane, timeout, retries) come from the call's optional parameters or defaults,
  not handler attributes; the per-cluster disabled-priority check must account for it.
- Security: invoking a method named in stored job data means anyone who can write to the master DB can call arbitrary
  static methods. Allow-list the types / assemblies registered with the adapter.
- Signature drift (method renamed / parameters changed while jobs are queued): fail with a clear "method not found".

## Job logging from the handler through `JobContext`

Raised 2026-11-14 (Hugo). Let a handler write its own log lines from the `JobContext` (e.g. `context.Log(level,
message)` / `LogInfo` / `LogWarning` / `LogError`). Only reachable through `JobContext`, so every line is written
during an execution: link it to that execution (Hugo), so it shows with the run in the dashboard. Logging outside a
handler is the app's own logger, not JobMaster's.

Keep handler lines distinguishable from the engine's own (failures, timeouts, etc.), e.g. a new log category or a flag
on the log item, so the dashboard can show them together but filter either out. Category naming: add a new member,
never rename `JobExecution` (stored by name: SQL writes `category.ToString()`; a rename orphans existing logs and breaks
the archive / purge). Candidates, keeping the execution link in the name: `JobExecutionHandler` (next to
`JobExecution`, makes the pairing obvious), `ExecutionHandler`, `JobExecutionUser` ("user" clashes with the dashboard's
logged-in user), `JobHandler` (doesn't say it's tied to an execution). Name and implementation still to be decided.

Open:
- Retention: `DeleteOldLogsRunner` skips `JobExecution`-category logs because they're archived / purged with their job
  in `DeleteOldFinalJobsRunner`. A new handler category needs the same treatment, or handler lines expire on the
  general log TTL while the job is kept.
- Reuse the cluster's `JobMasterLogger` (batched, `MaxQueueLimit` 10,000, Debug dropped) or a separate queue, so a
  chatty handler can't push out JobMaster's own logs.
- Limits: message size and lines per execution.
- What the user writes is their responsibility, but the docs should say these logs are stored and shown in the
  dashboard (don't log secrets / personal data).

## Optional dead-letter handler

Raised 2026-10-30. When a job or schedule can't be saved or read, the Critical log is today the only trace (and since
0.0.12 it carries ids and sizes only, never message data). SQL and RavenDB `PullMessagesAsync` delete on pull, so once
master save and the agent re-push both fail, the job exists only in memory (NATS naks and is redelivered, no loss):
- Save and re-queue both failed (`PollingSavePendingJobsRunner`, `PollingSaveRecurringScheduleRunner`, the drain
  safeguard in `JobSavePendingOperation`): **possible** loss, not certain. The master write may have committed before
  throwing, and in the batch-timeout path in-flight saves keep running (no token passed). The drain path returns
  `Failed`, but `PollingDrainSavePendingJobsRunner` only backs off; the job was already pulled and is not retried.
- Unreadable message on SQL / RavenDB: **certain** loss for that message (already pulled); since 2026-11-14 the rest
  of its batch is no longer lost with it.

Idea (not designed): an optional handler, no-op by default, e.g. `IJobMasterDeadLetterHandler` ("dead letter" rather
than "lost", since most cases may already be saved). Context carries ids, `IReadableMessageData`, metadata, a reason
(`SaveAndRequeueFailed` / `DrainSaveFailed` / `UnreadablePayload`) and the exception; raw payload for unreadable ones.
Call guarded (try/catch + timeout). Open: per-cluster registration (like `EnableMirrorLog`) vs global DI; one
interface vs three.

## NATS: keep save-pending messages that keep failing in a KV store

Raised 2026-11-16 (Hugo: future, not now). On SQL, a save-pending message leased `MaxSavePendingLeaseCount` (1000)
times is logged Critical and moved to `JMReserved-Terminated::<address>` (kept, never pulled again). NATS has no
such limit today, and `AckTerm` would drop the message. Idea: same rule on NATS, using JetStream's delivery count
(`NumDelivered` in the message metadata) instead of `lease_count`. Past the limit, write the message to a NATS KV
bucket (keyed by message id), log the same Critical summary (`ToLogSummary()`, never the payload), then `AckTerm`.

## NATS: `WorkQueue` retention instead of `Limits` + `MaxAge`

Raised 2026-11-16. Since 0.0.12 the stream keeps messages 30 days (`MaxAge`), handled or not, so a handled message still
uses disk for 30 days and a message waiting longer than that (all workers down) is deleted. `WorkQueue` retention
removes a message exactly when it's acked or `AckTerm`'d (checked on NATS 2.10: 3 → 0) and never one that can still be
delivered. Open: retention can't be changed on an existing stream (new stream + migration); one consumer per subject
(JobMaster has one per bucket subject, check overlaps); `AckTerm` then deletes, so keeping poison messages needs the
KV idea above.

## Generate the SQL schema script from the command line

Raised 2026-11-13, after plan item 24; not in 0.0.12. For users with `DisableAutoProvisionSqlSchema()` who manage the
schema themselves. The scripts are already plain strings from an `ISqlGenerator` + table prefix
(`MasterTableCreatorScripts`, `AgentTableCreatorScripts`, Postgres `CreateCaseInsensitiveCollationSql`), no connection
needed. Shape: a public method (e.g. `SqlSchemaScripts.Generate(provider, prefix, master, agent)`) plus an optional thin
`dotnet tool` (`dotnet jobmaster sql-script --provider postgres --prefix JM_ --master --agent`).

Open:
- Scripts aren't idempotent: existence is checked in C# (`SqlJobMasterRuntimeSetup`), so a dump is plain `CREATE TABLE`.
  Re-runnable output needs a per-provider `IF NOT EXISTS` form.
- Upgrades: auto-provision only creates missing tables, no ALTER/migration concept. Create-only covers first install;
  decide whether this grows into versioned upgrade scripts.
- Master and agent generated separately (different DBs/providers, hybrid legs); the master DB also needs the agent
  tables for the fallback connection (`ProvisionAgentTablesAsync` in `ConfigClustersAsync`).
- Scripts and generators are `internal`: public API in SqlBase, or `InternalsVisibleTo` for a tool project.

---

# Docs / benchmarks / positioning

## Docs: cluster settings are sticky, retry settings aren't

For the cluster configuration page (plan item 10). Shared cluster settings (`TransientThreshold`, `DefaultJobTimeout`,
`DefaultMaxRetryCount`, `MaxMessageByteSize`, `DataRetentionTtl`, `ClusterMode`, ...) are saved as
`clusterDefinition.X ?? stored`: removing a setting from code doesn't reset it, the stored value stays. To go back, set
it to the default once (e.g. `TransientThreshold(JobMasterDefaults.TransientThreshold)`), then the line can go. On
purpose: an instance that doesn't mention a setting must not reset it for everyone (and removing `.Mode(Archived)`
must not reactivate a cluster). The API's cluster endpoint shows what's stored.

Retry settings are the exception: never read back (each worker retries by its own code), so every process with a Full
or Execution worker overwrites the stored `RetryDelays` / `RetryInterceptor` at startup; removing `RetryDelays(...)` from the
workers goes back to the built-in delays. A worker replacing different stored settings logs it (Info, Cluster category);
if it keeps repeating, workers run different configurations.

## Dashboard: "Failure & Retries" panel compares failures with max retries

Seen in the sandbox 2026-11-14: a job with `MaxNumberOfRetries = 2` that failed every time shows "Number Of Failures 3"
next to "Max Number Of Retries 2" with a full red bar, which reads as one attempt too many. It's correct: 1 attempt +
2 retries = 3 attempts, all failed (`HasRetriesLeft` is `NumberOfFailures < MaxNumberOfRetries`; the last failure is
counted by `MarkAsFailed`). Show it as attempts ("3 of 3", i.e. `NumberOfFailures` / `MaxNumberOfRetries + 1`) or as
retries used ("2 of 2", `min(failures, max)` / `max`) with failures shown separately.

## Dashboard: show the cluster's retry settings

Since 2026-11-14 the API's cluster endpoint returns `RetryDelays` and `RetryInterceptor` (`None` / `Delegate` / the
interceptor type's full name), stored at startup by the last process running jobs (Full / Execution worker) that started. Show
them in the dashboard's cluster view, with a note that each worker retries by its own configuration and the failed
execution's message records what was actually applied.

## `DashboardOAuthEndpoints`'s `redirect_uri` derives from `ctx.Request.Host` — fine, but undocumented

Same Host-derived pattern in `BuildCallbackUrl`/`ResolveUrl` (`DashboardOAuthEndpoints.cs`), but not a bug
here — the OAuth provider itself rejects a `redirect_uri` that doesn't exactly match what's registered for
the Client ID, so a forged `Host` alone doesn't work. Only becomes a real issue if the provider allows
loose/wildcard redirect matching, or the admin registered one. Gap is just doc: nothing tells admins to
register the exact, full callback URL. Worth a line in the OAuth setup docs.

## Benchmarks

- **RavenDB Community license caps resources**: the Community license limits a cluster to **3 CPU cores and 6 GB RAM**
  whatever the container gets (ravendb.net/buy), so RavenDB benchmarks run with less real capacity than the other
  (uncapped) providers. Seen 2026-08-22: going to 8 CPU / 8 GB didn't help RavenDB the way it helped SQL Server, and
  job loss grew with volume (2,579 → 7,432 → 10,495 at 25k/50k/100k). Keep in mind for any capacity comparison.
- **Re-run the RavenDB+NATS benchmark with capped (not unlimited) worker CPU/memory**: Local testing (2026-09-07/08)
  found that uncapped worker CPU quietly made `Environment.ProcessorCount` inside each worker container report the full
  host core count instead of 1-2, which unlocked dramatically higher execution throughput (~90/sec → ~330+/sec) — but
  running many workers fully uncapped simultaneously (10+) reliably collapses the host (containers fail to
  start/answer, all schedule requests time out). The safer, repeatable middle ground found so far: cap workers at
  **2 CPU / 4GB each**, DB at **3 CPU / 6GB**, NATS at **3 CPU / 4GB** — gives most of the throughput benefit without
  the collapse risk. Next step: repeat this properly (3x-averaged, not single runs) on a dedicated machine with 20
  workers once a suitably sized box is provisioned (a DigitalOcean CPU-Optimized droplet, 48 vCPU/96GB, ~$1.50/hr, was
  sized for this — 20 workers x 2 CPU = 40 vCPU + DB/NATS headroom).
