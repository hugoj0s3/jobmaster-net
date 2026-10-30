# Plan 0.0.12 Alpha

Original notes: [Plan0.0.12Alpha.original.md](Plan0.0.12Alpha.original.md).

Each item is meant to be picked up on its own, in order. Every item has: goal, current state, design,
open decisions (settle before coding), tasks, and done criteria.

**Delivery:** one PR per item, merged into the `0.0.12-alpha` branch. No rush — the version can take ~1 month.

| #  | Item                                                  | Size  | Breaking | Depends on |
|----|-------------------------------------------------------|-------|----------|------------|
| 1  | Cluster id via attribute / `JobDefinitionConfig`      | S     | **Yes** (param order, attribute inheritance) | – |
| 2  | NaturalCron upgrade + string overloads                | S     | No       | –          |
| 3  | Cronos/NCrontab colliding method names                | S     | No (obsolete) | 2     |
| 4  | `GetJobHandlerTypeFromId` single-scan cache           | XS    | No       | –          |
| 5  | Bootstrap-plumbing public/internal audit              | XS    | Maybe    | –          |
| 6  | `JobScheduleReceipt` scheduler contract               | M     | **Yes**  | 1          |
| 7  | Confirm on master, explicit (`ConfirmOnMasterAsync`)  | S     | No       | 6          |
| 8  | Hybrid agent connection                               | L     | No       | own sub-plan file first |
| 9  | Reminders cleanup                                     | XS    | –        | 1–8        |
| 10 | Docs update (jobmaster-doc)                           | M     | –        | 1–9, 12, 13, 15–19 |
| 11 | Docs restructure + `presenting.md`                    | M     | –        | 10         |
| 12 | Case-insensitive metadata/message-data keys + worker lanes | M | **Yes** | 1          |
| 13 | Disabled-priority validation scoped per cluster       | S/M   | Maybe (stricter recurring check) | 1 |
| 14 | Startup type scans: one pass + safe `GetTypes`        | S     | No       | 1 (13 adds a scan user) |
| 15 | Consistent cancel / reschedule method names           | XS    | No (obsolete aliases) | – |
| 16 | `CancelAsync` on receipts (follow-up to 15)           | S     | No       | 6, 7, 15   |
| 17 | Base64 ids in logs (messages + API `ReferenceId`)     | S/M   | API response (`ReferenceId` format) | – |
| 18 | Archived safety net: strict mode flow + stray sweeper | S/M   | **Yes** (Active → Archived refused) | – |
| 19 | Startup: validate everything before anything starts   | M     | No (errors move earlier) | 13 (its static check moves here) |
| 20 | `JobMasterAssemblyDiscovery()` for unreferenced libraries | S  | No       | 14 (same branch) |
| 21 | Load provider assemblies by repo type (bug fix)       | XS    | No       | 14         |
| 22 | Lambda adapter: reserved definition-id prefix (idea)  | –     | –        | new project; scope TBD |

---

## 1. Cluster id configurable via attribute (like lane, priority, etc.)

**Goal.** When `clusterId` isn't passed explicitly, resolve it from the handler/definition before falling
back to the default cluster — same pattern as worker lane.

**Current state.**
- `JobMasterScheduler.ResolveClusterId` ([JobMasterScheduler.cs](src/JobMaster/JobMasterScheduler.cs)) only does
  explicit → `JobMasterClusterConnectionConfig.Default`.
- Lane resolution precedence lives in `JobUtil.GetWorkerLane` ([JobUtil.cs](src/JobMaster/Sdk/Abstractions/Jobs/JobUtil.cs)):
  explicit → applied `JobDefinitionConfigAttribute` config → `[JobMasterWorkerLane]`.
- `JobDefinitionConfig` has no `ClusterId`; `ApplyOverrides` doesn't know about cluster id.
- Static recurring schedules already resolve cluster from `IStaticRecurringSchedulesProfile.ClusterId` →
  `defaultClusterId` (`RecurringScheduleDefinitionCollection`).

**Design.**
- New `JobMasterClusterIdAttribute(string clusterId)` next to `JobMasterWorkerLaneAttribute`.
- New `JobDefinitionConfig.ClusterId`, the first optional ctor param, right after the id. **Decided:** a documented breaking change. In every scheduling method `clusterId` also moves from last to right after the payload (`msgData`/`data`).
- New `JobUtil.GetClusterId(Type handlerType, string? clusterId)`: explicit → applied
  `JobDefinitionConfigAttribute` config → `[JobMasterClusterId]` → default cluster.
- `JobMasterScheduler`: `NewJob<T>`/`NewRecurSchedule<T>` use `GetClusterId(typeof(T), …)`; config-based
  overloads use explicit → `config.ClusterId` → default. Resolve **once** at the top of each public method
  and pass the resolved id down (today `EnsureCanSave` re-resolves from the raw `clusterId` argument —
  that would silently validate against the default cluster instead of the attribute one).

**Decided.**
- Static recurring precedence: profile `ClusterId` → handler (`JobDefinitionConfigAttribute` /
  `[JobMasterClusterId]`) → default. Same rule as lane.
  - Lane already works this way, at spawn time: the collection stores only `profile.WorkerLane`, then
    `Job.FromRecurringSchedule` → `JobUtil.GetWorkerLane` falls back to the handler attribute.
  - Cluster id **can't** be resolved at spawn time — the schedule itself is stored on that cluster — so it
    has to be resolved at registration in `RecurringScheduleDefinitionCollection.Add` (and in its
    `EnsureUnique`/`GenerateUniqueId`, which currently re-derive the cluster from the profile only).

**Open decisions.**
- Startup validation (hard rule): an attribute/`JobDefinitionConfig` cluster id that isn't registered must
  fail in `DefaultRuntimeValidatorSetup`, not at first schedule call. Also add `JobMasterClusterIdAttribute`
  to the "don't mix with `JobDefinitionConfigAttribute`" check there.
- ~~`CancelJob`/`ReSchedule`/`CancelRecurring` cluster fallback~~ — **decided:** accepted and documented
  (XML docs). They act by id only, so a non-default cluster must be passed explicitly.

**Tasks.**
- [x] Attribute + `JobDefinitionConfig.ClusterId` + `ApplyOverrides` carries it through.
- [x] `JobUtil.GetClusterId`; wire into `JobMasterScheduler` (all `Once*`/`Recurring*`, sync + async, both families).
- [x] Static recurring: `RecurringScheduleDefinitionCollection` respects handler attribute per decided precedence.
      (`StaticRecurringSchedulesProfileInfo.ClusterId` is now nullable = "profile declared none"; bootstrap
      validates/upserts per definition's `ClusterId`, since one profile can now span clusters.)
- [x] Validator: unknown cluster id (handler types + every `IStaticJobDefinitionConfig` implementation,
      incl. publisher-only ones) + attribute-family mixing.
- [x] Unit tests: precedence matrix (explicit / definition config / attribute / default) for jobs, dynamic
      recurring, static recurring, and the Advanced `TDefinition` overloads; validator failures.

**Done when.** Precedence matrix tests green; full unit suite green.

---

## 2. NaturalCron upgrade

**Goal.** Move to the newest NaturalCron on NuGet (fix from https://github.com/hugoj0s3/NaturalCron/pull/8).

**Current state.** `JobMaster.csproj` references `NaturalCron 1.0.1`. Usage is confined to
`src/JobMaster/RecurrenceExpressions/NaturalCron/*` and `NaturalCronScheduleAttribute`.

**Tasks.**
- [x] Read PR #8 + NaturalCron changelog; note any API/behavior changes (parse errors, `TryParse` shape,
      next-occurrence semantics). (→ 1.1.0: no API change; PR #8 only touched the UTC methods JobMaster doesn't
      call; 1.1.0's occurrence fixes do change results — see ChangeLog.)
- [x] Bump version; build; adjust `NaturalCronExprCompiler`/`NaturalCronCompiledExpr` if the API moved. (No change needed.)
- [x] Add a regression unit test for the specific case PR #8 fixed. (PR #8's fix is in UTC methods JobMaster doesn't
      call, so it can't be exercised through JobMaster; the test pins the equivalent guarantee on JobMaster's own
      `TryGetNextOccurrenceInTz` + UTC-conversion path. Plus one for a 1.1.0 fix, verified to fail on 1.0.1.)
- [x] Existing `NaturalCronExprExtensions` methods take `NaturalCronExpr`, not `string`, so they don't collide
      and keep their names. Only the **new** string-taking overloads (from the reminder) get the prefixed
      name: `NaturalCronRecurringAsync<T>(string)`/`NaturalCronRecurring<T>(string)`/`NaturalCronAdd<Th>(string)`.

**Done when.** Builds, existing NaturalCron tests + new regression test green.

---

## 3. Cronos/NCrontab colliding method names

**Current state.** `CronosExprExtensions` and `NCrontabExprExtensions` both expose
`Recurring<T>`/`RecurringAsync<T>`/`Add<Th>` taking `string` → ambiguous when both packages are referenced.
Auto-detection was already rejected (both parse the same syntax).

**Design.** Prefixed names for string-taking methods only (`CronosRecurringAsync<T>`, `NCrontabAdd<Th>`, …;
same convention as item 2's new NaturalCron string overloads). Old Cronos/NCrontab names stay as
`[Obsolete]`; dedup bodies only when obsoletes are removed.

**Tasks.**
- [x] Add prefixed methods, obsolete old ones, update samples/benchmarks to the new names. (Only
      `TargetTestRecurringApp` + NuGet READMEs used them.)
- [x] Unit test: a project referencing both providers compiles a call to each (or at least both prefixed
      methods resolve).

---

## 4. `JobMasterDefinitionIdAttribute.GetJobHandlerTypeFromId` re-scans assemblies

**Current state.** On a cache miss it scans all assemblies but caches only the requested id. Called on the hot
path (`JobsExecutionEngine`, `RecurringSchedulePlanner`). Since item 13 also from `JobUtil.GetHandlerPriority`
(`Recurring<T>` / `RecurringAsync<T>` without a priority, and the static-schedule bootstrap). A miss is never cached, so
on a publisher-only app with no handler for that id, every such call rescans everything. Done together with item 14
(Hugo, 2026-10-30).

**Tasks.**
- [x] On a miss, map every handler of each not-yet-mapped assembly at once (via `GetJobDefinitionId`), under a lock;
      lookups stay lock-free on the `ConcurrentDictionary`.
- [x] Unknown id (Hugo): recheck only assemblies loaded since the last miss; none new → `null` without reading types.
- [x] Unit tests: one `GetTypes()` per assembly (`TypeDiscoveryTests`); resolve by config / `[JobMasterDefinitionId]` /
      full name; unknown id → `null`, twice.

---

## 5. Bootstrap-plumbing public/internal audit

**Current state.** The reminder says `JobMasterScheduler` was made `internal` on 2026-09-19, **but
[JobMasterScheduler.cs:18](src/JobMaster/JobMasterScheduler.cs#L18) is still `public class`** on this branch —
the change seems to have been lost. Redo it first.

**Tasks.**
- [x] Make `JobMasterScheduler` internal again; rebuild solution.
- [x] Grep for other concrete classes that only exist for `BootstrapBlueprintDefinitions` / DI bootstrap and are
      public; make internal where nothing outside `InternalsVisibleTo` uses them. (None left: `JobMasterRuntime`,
      `JobMasterRuntimeSingleton`, `DefaultJobMasterMessageJsonSerializer` were already internal; the remaining
      public classes are user-facing API.)
- [x] Note any public→internal change in ChangeLog (technically breaking).

---

## 6. Scheduler contract: `JobScheduleReceipt`

**Goal.** Tell the caller *where* the job landed: accepted by the transport (agent, pending save to master) or
already stored on master.

**Was.** Every scheduling method returned `JobContext` / `RecurringScheduleContext`, and
`JobMasterSchedulerClusterAware.Schedule*` returned `void`/`Task` while taking one of two paths:
- bucket found → `AssignSavePendingJobToBucket` + `agentJobsDispatcherService.AddSavePendingJob…` → **AcceptedOnTransport**
- `ClusterMode.Migrating` / no bucket / bucket without `AgentWorkerId` → `MarkAsHeldOnMaster` + master add → **StoredOnMaster**

**Design (implemented).**
```csharp
public enum ScheduleReceiptStatus { AcceptedOnTransport, StoredOnMaster }

public sealed class JobScheduleReceipt
{
    internal readonly JobRawModel model;                    // kept for ConfirmOnMasterAsync (item 7)
    internal JobScheduleReceipt(ScheduleReceiptStatus status, JobRawModel model);

    public ScheduleReceiptStatus Status { get; }
    public string ClusterId { get; }
    public string? BucketId { get; }                        // null when StoredOnMaster
    public string? AgentWorkerId { get; }                   // null when StoredOnMaster
    public string? AgentConnectionId { get; }               // null when StoredOnMaster
    public JobContext Context { get; }                      // built from the model on first read, then cached
    public Task ConfirmOnMasterAsync();                     // item 7
}

public sealed class RecurringScheduleReceipt { /* same shape, Context is a RecurringScheduleContext */ }
```
- `JobMasterSchedulerClusterAware.Schedule*` returns `BucketModel?`: the bucket it was handed to, `null` = stored on
  master. `JobMasterScheduler` turns it into the receipt (`ToReceipt(rawModel, bucket)`).
- Receipt fields are captured in the constructor, so later changes to the model (item 7) don't change what the
  receipt shows. Hot-path cost: one receipt object per call, nothing else.
- Sync methods return the receipt, async return `Task<receipt>`; no `CancellationToken` (the scheduler has none).

**Decided.** Hard break, no `[Obsolete]` shims. ChangeLog + docs migration note:
`var ctx = scheduler.OnceNow<T>()` → `scheduler.OnceNow<T>().Context`.

**Tasks.**
- [x] Types + XML docs.
- [x] `IJobMasterScheduler`, `IJobMasterSchedulerAdvanced`, `JobMasterScheduler` (all overloads).
- [x] `IJobMasterSchedulerClusterAware` returns the bucket.
- [x] Extension classes: NaturalCron / TimeSpan / Cronos / NCrontab.
- [x] Callers: benchmarks, scenario-test apps (JobMaster.Api and samples don't use the return value).
- [x] Unit tests: both statuses for jobs + recurring (Migrating mode, no bucket, normal bucket).
- [x] ChangeLog breaking-change entry.

---

## 7. Confirm on master (explicit, on demand)

**Goal.** Let a caller who needs it make sure a job or recurring schedule is saved on master, instead of only on
the agent transport (lost if the transport dies before the agent's save to master).

**Dropped: automatic flush.** The first design kept every `AcceptedOnTransport` item in an in-memory buffer and
flushed it to master on an adaptive background timer (soft/hard age thresholds, shutdown drain, `ProcessExit` hook).
It was built and reverted: too complex, and it cost memory and work on every scheduling call. Jobs nobody confirms
keep today's behavior; the "Cluster-aware bulk persistence" idea in [src/JobMaster/Reminders.md](src/JobMaster/Reminders.md)
stays open.

**Design (implemented).** Nothing happens unless the user calls it.
- `receipt.ConfirmOnMasterAsync()` on both receipts, and `scheduler.Advanced.ConfirmOnMasterAsync(receipts)` (jobs /
  recurring overloads) on `IJobMasterSchedulerAdvanced`: kept under Advanced on purpose, so it's less visible.
  All return `Task`.
- `StoredOnMaster` receipts: no call to master. The rest are grouped by cluster (ignoring case), then
  `JobMasterSchedulerClusterAware.Confirm*OnMasterAsync`:
  - job → `MarkAsHeldOnMaster()`, schedule → `Active()`;
  - `Partition(GetMasterMaxBatchSize)`, one `BulkInsertIfNotExistsAsync` per partition (new master-service methods
    over the existing repository methods from the archive feature; no status rule).
- Why held on master / Active: every agent save inserts first and treats a duplicate as already handled. If the
  confirm wins, the agent drops its message, so the row must be one master's own runners pick up
  (`AssignJobsToBucketsRunner` only takes `OnMaster`). A `PendingSave` row would never run.
- Fixed: the recurring save path (`RecurringScheduleSavePendingOperation`) rethrew `JobMasterDuplicationException`
  and the runner re-queued it forever; now treated as already saved, like the job path, and still enqueued for the
  recently-inserted fast path (so a schedule confirmed before the agent's save is still planned within ~1s).
- Not-inserted ids are checked with a bulk `QueryExistingIdsAsync`; any id neither inserted nor on master throws
  (covers RavenDB's per-row fallback that logs and continues). Archived cluster → throws before writing.
- A master error is thrown; calling again is safe (existing rows are left untouched).

**Decided.**
- Not recommended unless the caller needs the guarantee: a confirmed job may start later, since it's picked up from
  master instead of the short-circuit straight to its bucket. Documented in the XML docs.
- No late-confirm guard: JobMaster is at-least-once. Confirming a receipt after data retention purged an
  already-finished job inserts it again and it runs again; accepted and documented (only possible with a
  `DataRetentionTtl` set, default is no purge).
- No `MasterConfirmationResult` / timeout: one database call per partition; success or exception.
- Standard: partition by `GetMasterMaxBatchSize` before any bulk master call. Callers that don't yet (migrate +
  archive runners) are in [ChangeLog/reminders.md](ChangeLog/reminders.md).

**Tasks.**
- [x] `BulkInsertIfNotExistsAsync` on `IMasterJobsService` / `IMasterRecurringSchedulesService`.
- [x] Confirm methods (receipts, Advanced batch, cluster-aware) + partitioning.
- [x] Recurring duplicate fix.
- [x] Unit tests: StoredOnMaster skipped; AcceptedOnTransport marked held/Active and inserted; grouping across
      clusters; partitioning; neither-inserted-nor-existing throws; archived throws; recurring duplicate no longer
      re-queued and still enqueued.
- [x] RepoConformance: `BulkInsertIfNotExistsAsync` with non-final (`OnMaster` / `Active`) rows returning only the
      inserted ids, and `QueryExistingIdsAsync`, all providers.
- [x] Scenario test: confirm right after scheduling → job reaches master and executes once
      (`ConfirmOnMasterTest/PostgresPure`, jobs + recurring).
- [x] ChangeLog (added). Docs: item 10.

---

## 8. Hybrid agent connection

**Status.** In scope for 0.0.12, but the biggest item — gets its own dedicated sub-plan file
(e.g. `PlanHybridAgentConnection.md`) before coding. What's below is the groundwork already decided, to
carry over into that file. May itself span more than one PR (e.g. repo-type split helper + call sites first,
then the `.Hybrid()` config API + fingerprint, then validation).

**Goal.** One agent connection that uses one provider for transport (save/dispatch) and another for
execution, e.g. NATS for transport + RavenDB for execution. Replaces the per-worker sketch in the reminder.

```csharp
AddAgentConnection("hybrid-connection-1")
    .Hybrid()
    .Transport(["nats-connection"])
    .Execution(["raven-db"]);
```
Workers attach to the hybrid connection like any other.

**Decided: one shared format for RepoType and fingerprint**, always built by the framework (never by a provider):
```
hybrid:<transport>&&<execution>
RepoType:    hybrid:NatsJetStream&&RavenDb
Fingerprint: hybrid:<nats-fp>&&<ravendb-fp>
```
- Role order is fixed (transport first), so swapping roles changes both strings → `ProtectConnectionChanges`
  catches it. Changing either underlying connection also changes the hybrid fingerprint.
- Separator decided: `&&` (reads as "both"; two chars so it won't appear by accident; unlike `+`, it doesn't
  silently decode to a space in a query string). `&&` and the `hybrid:` prefix become **reserved**. Hard
  validation, with explicit throws:
  - any `IAgentFingerprintResolver` returning a fingerprint containing `&&` or starting with `hybrid:` → throw
    (today all resolvers return GUIDs: Sql `ToString()`, NATS/RavenDB `ToString("N")`, so nothing breaks);
  - any provider `RepositoryTypeId` containing `&&`/`:` → throw at registration
    (current ids: `Postgres`, `MySql`, `SqlServer`, `NatsJetStream`, `RavenDb` — all fine).
- Encoding: any Dashboard/Api place that puts repo type or fingerprint in a URL must URL-encode it (`&` splits
  query params); anything building HTML by hand must escape `&`. Add a test for the URL case if it exists.

**Storage length check (done).**
- Master: the agent connection's `Fingerprint`/`RepositoryTypeId` live in `AgentConnectionRecord`, serialized as
  JSON in the master generic-record table (`MasterAgentConnectionService.SaveConnectionAsync`) — no column
  length limit.
- Agent SQL: `agent_conn_fingerprint.fingerprint` is `varchar(250)`
  ([AgentTableCreatorScripts.cs:88](src/Providers/JobMaster.SqlBase/Scripts/AgentTableCreatorScripts.cs#L88)).
  Each underlying connection keeps writing its own GUID there; the composite only goes to master. Even if it
  were stored there, `hybrid:` + 2×36 + sep ≈ 82 chars, well under 250.
**Hardest part: repo-type-keyed lookups.** Everything keyed by repo type must check "is this connection
hybrid?" first, split it, and call the lookup with the right role's repo type — never with the hybrid string.
- `SqlGenerator.Get(repositoryTypeId)` — expected to be the most complex change: callers today assume one
  connection = one SQL dialect; with hybrid, each caller has to know which role (transport/execution) it is
  working for before picking the generator.
- `ClusterServiceKeys.GetFingerprintResolverKey` — needs a hybrid resolver that composes the two.
- `KnownExceptionIdentifier`'s `byRepoType`, `JobMasterIocRegistrationAttribute` provider lookup,
  `ConnectionOptionsBinderFactory`.
- Likely shape: one central helper (e.g. `HybridRepoType.TryParse(repoType) → (transport, execution)?`) used at
  every call site, rather than ad-hoc string splitting. First step of the dedicated plan: inventory every
  call site and assign it a role.

**Open decisions (for the dedicated plan).**
- **IoC:** which agent repository interfaces resolve from the transport provider vs the execution provider —
  list every agent-side interface and assign a role.
- **Validation:** e.g. NATS' 5-min `TransientThreshold` requirement applies only when NATS is the execution
  side. Inventory every per-provider validation rule and decide per role.
- Coordinators must still declare all agent connections, including hybrid ones (see existing behavior).
- Throttler settings per role (ties into the per-transport throttler idea).

**Tasks.**
- [ ] Create the dedicated sub-plan file and move this section into it.
- [ ] Inventory every repo-type-keyed call site and assign it a role (transport/execution).
- [ ] Settle the open decisions above; split the work into PRs in the sub-plan.

---

## 9. Reminders cleanup

There are two reminder files: [ChangeLog/reminders.md](ChangeLog/reminders.md) and
[src/JobMaster/Reminders.md](src/JobMaster/Reminders.md).

- [ ] Remove items finished by this release (3, 4, 5, hybrid sketch → replaced by item 8). Keep
      "Cluster-aware bulk persistence": item 7 dropped the automatic flush, so that gap is still open.
- [ ] Re-check older items against code (e.g. `AcquireAndFetchAsync` deadlock part, RavenDB timeout).
- [ ] Decide whether to merge the two files into one.

---

## 10. Update documentation (`C:\Users\hugo8\RiderProjects\jobmaster-doc`)

- [ ] Document everything from items 1–8 (attribute precedence, `JobScheduleReceipt`, `ConfirmOnMasterAsync`
      caveats, renamed cron methods, NaturalCron string overloads).
- [ ] Re-verify enum names in prose against the C# source (known drift: `JobMasterJobStatus`, `BucketStatus`,
      `ClusterMode`).

## 11. Restructure documentation

- [ ] Restructure around standalone mode first: install → first job → recurring → dashboard, then distributed.
- [ ] Create `presenting.md` (reference for a later article).

---

## 12. Case-insensitive metadata and message-data keys, and worker lanes

**Goal.** Match ignoring case everywhere, consistent with cluster ids (item 1):
- **Metadata and message-data (`MsgData`) keys:** `"Category"` and `"category"` are the same key for overrides, for
  reads (`TryGetStringValue("CATEGORY")`) and for API/DB filters.
- **Worker lanes:** `"Billing"` and `"billing"` are the same lane in every comparison, API filter and DB query, not just
  in bucket routing.

**Current state (found during item 1).**
- In memory, keys are case-sensitive. `KeyValueBag` (behind `Metadata`/`WritableMetadata`), `JobUtil.GetHandlerMetadata`
  and `JobMasterDictionaryUtils.Merge` all use a plain `Dictionary<string, object?>`.
  - So a derived `[JobMasterMetadata("category", ...)]` does **not** override a base `"Category"`; the job gets both
    keys.
  - Call metadata only overrides an attribute key when the case matches exactly.
- DB metadata filters (API `metadata` filter, e.g. the scenario tests' `TestIdentifier`) compare the key with a plain
  SQL `=` on the generic-record key column. Not verified yet, but by provider defaults:
  - Postgres: case-sensitive.
  - SQL Server / MySQL: case-insensitive default collations.
  - RavenDB: case-insensitive string equality.
  - So the same filter likely behaves differently per provider.
- **Worker lanes are only partly case-insensitive.**
  - Already ignoring case:
    - bucket routing (`MasterBucketService.FilterApplicableBuckets`, `OrdinalIgnoreCase`), so
      `[JobMasterWorkerLane("Billing")]` reaches a `"billing"` worker;
    - `PreValidation`, which rejects two spellings of one lane across a cluster's workers (`NormalizeId`).
  - Still exact or DB-dependent:
    - **Bug:** API `GET /workers` filters lanes with `Ordinal` (`WorkersEndpoints.cs:37`), while `/workers/count`
      uses `OrdinalIgnoreCase` (`:79`). So with `?workerLane=Billing` the list is empty while the count isn't.
    - `MasterBucketService` bucket query filter: `b.WorkerLane == criteria.WorkerLane`.
    - SQL job/recurring-schedule queries `WorkerLane = @WorkerLane`, which depend on collation (same issue as metadata
      filters; check RavenDB too).
    - `AssignJobsToBucketsRunner.BucketFailureKey` (`{cluster}_{lane}_{priority}`): separate no-bucket fallback timers
      per spelling.
    - Standalone drainer lanes in `JobMasterRuntime` (`.Distinct()` over existing buckets): could start two drainers
      for buckets left under two spellings, e.g. after a lane's case changed between deployments.
  - Jobs store the lane spelling from the attribute or call. Unlike cluster ids it can't be normalized to a configured
    spelling, because a publisher-only app has no workers and so no lane registry. "Case-insensitive" for lanes means
    every comparison and DB filter ignores case, with no normalization.

**Design (to settle).**
- **Decided (Hugo):** message data too, not just metadata. Both sit on `KeyValueBag`, so the comparer goes there
  once and no metadata-only switch is needed.
- `KeyValueBag` → `StringComparer.OrdinalIgnoreCase`, including the `KeyValueBag(IDictionary)` constructor, which today
  wraps whatever dictionary it's handed (e.g. one deserialized from storage). It must copy into an ignore-case
  dictionary instead. Watch for an incoming dictionary that already holds two spellings of one key.
- Check every other path that builds or reads message data or metadata without `KeyValueBag`: serialization round-trips
  (master/agent storage, NATS/RavenDB documents) and the API/Dashboard models. Keys must come back as written, while
  lookups ignore case.
- `GetHandlerMetadata` / `Merge` use the same comparer, so derived-overrides-base works across casing.
- DB filters: make every provider match keys case-insensitively, e.g. `lower()`/`ILIKE` on Postgres, and confirm
  SQL Server/MySQL/RavenDB.
- Which casing is stored when two spellings collide: probably the last writer's casing, as with "last wins" values.
- Worker lanes: `OrdinalIgnoreCase` for every in-memory comparison, key and `Distinct`/`GroupBy`. DB lane filters
  ignore case on every provider, with the same per-provider approach as metadata filters. Check that the lane indexes
  (`idx_*_cluster_status_worker_lane`, `idx_*_cluster_worker_lane`) still get used if a filter switches to
  `lower(...)`, e.g. Postgres may need an expression index or a case-insensitive (`citext`/collation) column.

**Open decisions.**
- Recurring metadata precedence (also found in item 1). `Job.FromRecurringSchedule` merges
  `Merge(recurringMetadata, handlerMetadata)`, so the handler's `[JobMasterMetadata]` overrides the schedule's own
  metadata. One-off jobs are the opposite: call metadata wins.
  - `JobTests.FromRecurringSchedule_CopiesSchedulingAndMergesMetadataWithJobWinning` asserts this on purpose.
  - But it contradicts `recurring-schedule.md` ("configuration in a profile / `RecurringAsync` overrides handler
    attributes") and the same test's priority/timeout/lane, where the schedule wins.
  - Decide here, since it's the same merge code.

**Tasks.**
- [ ] Verify the current per-provider DB filter behavior first: a RepoConformance test, metadata filter with a
      different-case key, against all providers.
- [ ] Case-insensitive comparer in `KeyValueBag` (metadata + message data) / `GetHandlerMetadata` / `Merge`.
- [ ] Provider filter changes so all providers agree; RepoConformance test asserting case-insensitive matching on each.
- [ ] Unit tests: derived `"category"` overrides base `"Category"`; metadata and message-data reads ignore case; call
      metadata overrides an attribute across casing; storage round-trips keep the original key spelling.
- [ ] Worker lanes:
  - [ ] Fix `GET /workers` (`Ordinal` → `OrdinalIgnoreCase`) so it matches `/workers/count`, with an API test.
  - [ ] Make the other in-memory lane comparisons and keys ignore case: `MasterBucketService` query filter,
        `BucketFailureKey`, standalone drainer `Distinct`.
  - [ ] Make provider lane filters ignore case for jobs and recurring schedules. Add a RepoConformance test per
        provider, and confirm the lane indexes are still used.
  - [ ] Unit tests: a job with lane `"Billing"` is found and filtered under `"billing"` everywhere.
- [ ] Docs (`job-configuration.md` Message Data + Metadata + Worker Lane sections) + ChangeLog breaking entry.

**Done when.** The case-insensitive tests (keys and lanes) are green on every provider, plus the full unit suite.

---

## 13. Disabled-priority validation scoped per cluster

**Goal.** A priority disabled on one cluster (`DisablePriority(...)`) only affects jobs and schedules that go to that
cluster. Each check uses the fully resolved priority: explicit, then `JobDefinitionConfig`, then `[JobMasterPriority]`
(inherited, item 1), then Medium.

**Current state (found during item 1).** Scenario: cluster-b disables Critical, the default cluster allows it.
- **Startup, `JobMasterRuntime.PreValidation` (the `DisabledPriorities` block): wrong scope in both directions.**
  - False positive: for every cluster with disabled priorities it loops over every handler in the AppDomain, with no
    cluster filter. A `[JobMasterPriority(Critical)]` handler that only targets the default cluster fails startup
    because of cluster-b. Predates item 1, but `[JobMasterClusterId]` now makes it scopable.
  - False negative: it only reads `[JobMasterPriority]`, not `JobDefinitionConfig.Priority`. A definition-config
    handler with `priority: Critical` aimed at cluster-b passes startup.
- **One-off scheduling, `JobMasterScheduler.EnsureCanSave`: correct.** It validates against the job's resolved cluster
  (fixed in item 1) and throws `InvalidOperationException` at the call, including for explicit `clusterId:` routing.
- **Recurring: not checked when priority comes from the handler.**
  - `EnsureCanSave` (recurring) and the static bootstrap check (`BootstrapStaticRecurringSchedules`) only validate an
    explicit `Priority`. A null priority is resolved per occurrence by design.
  - `RecurringSchedulePlanner` has no priority check.
  - An occurrence that resolves to a disabled priority finds no bucket (`BucketQty` is forced to 0 for disabled
    priorities), waits `NoBucketFallbackThreshold` (2.5 min), then runs in the fallback bucket. Nothing fails; every
    occurrence is silently delayed.
- Medium can't be disabled (`ClusterDefinition.DisablePriority` throws), so the "no priority anywhere → Medium" default
  is always allowed.

**Design.**
- Startup: per handler, resolve its default cluster (`JobUtil.GetClusterId(t, null) ?? default cluster`) and its
  priority (`JobUtil.GetJobMasterPriority(t, null)`, which covers `JobDefinitionConfig`). Fail only if that priority is
  disabled on **that** cluster.
  - Move the check into `DefaultRuntimeValidatorSetup` as a testable `internal static` helper, like
    `ValidateClusterIds`, so errors are aggregated instead of thrown one at a time.
  - Explicit `clusterId:` routing stays covered by `EnsureCanSave`.
- Static recurring: validate each definition's resolved priority (explicit, else the handler's) against its resolved
  cluster at bootstrap. Better: before workers start, which ties into the item-1 lesson about bootstrap failures
  after `worker.StartAsync()`.

**Decided (Hugo, 2026-10-29).**
- Dynamic recurring with a handler-resolved priority: **fail at registration** (`Recurring<T>` / `RecurringAsync<T>`).
  The handler priority is resolved for the check only, not stored, so each run still resolves it.
- A definition-only `IStaticJobDefinitionConfig` with a priority disabled on its own `ClusterId` **is a startup
  error**, like item 1's cluster-id validator.

**Tasks.**
- [x] Per-cluster startup validation helper (`DefaultRuntimeValidatorSetup.ValidateDisabledPriorities`) + unit tests:
  - Critical handler on the default cluster with Critical disabled only on cluster-b → OK.
  - The same handler with `[JobMasterClusterId("cluster-b")]` → error.
  - `JobDefinitionConfig` priority checked.
  - Inherited `[JobMasterPriority]` checked.
  - Definition-only config checked; unknown cluster skipped.
- [x] Remove the old all-handlers loop from `PreValidation`.
- [x] Static recurring: resolved-priority check per definition cluster (`Priority ?? JobUtil.GetHandlerPriority`).
      Still at bootstrap after the workers start, like before; no unit test (private bootstrap path).
- [x] Dynamic recurring: `EnsureCanSave` resolves `Priority ?? GetHandlerPriority` + tests. The definition-config path
      on a publisher-only app has no handler, so it keeps checking only an explicit / config priority.
- [x] ChangeLog. Docs (`cluster-configuration.md` DisablePriority section, `job-configuration.md` priority warning):
      item 10.

**Done when.** The per-cluster priority tests and the full unit suite are green.

---

## 14. Startup type scans: one pass and a safe `GetTypes`

**Goal.** Startup enumerates the AppDomain's types once per scanning component instead of once per check, and an
assembly whose types can't be loaded never crashes startup.

**Current state (found in the item-1 review).**
- `DefaultRuntimeValidatorSetup.ValidateAsync` builds `assemblies` once, but runs `assemblies.SelectMany(a => a.GetTypes())`
  four times. `GetTypes()` allocates a fresh array on every call.
  - Missing-`Config` `IStaticJobDefinitionConfig` check.
  - `IJobMasterHandler` types: duplicate ids, single-definition and mixing checks.
  - `IStaticJobDefinitionConfig` types again, for `ValidateClusterIds` (added in item 1).
  - `IStaticRecurringSchedulesProfile` types, for `ValidateStaticProfileClusterIds` (added in item 1).
- Type-load failures are handled three different ways:
  - **Unprotected**, so a `ReflectionTypeLoadException` (e.g. an assembly with a missing dependency) fails startup: the
    four validator scans, `ClusterConfigBuilder` (runtime-setup and handler registration),
    `JobMasterIocRegistrationAttribute`, and `JobMasterDefinitionIdAttribute.GetJobHandlerTypeFromId`.
  - **Bare `catch` → `Array.Empty<Type>()`**, which drops the whole assembly, including its loadable types:
    `JobMasterRuntime` (disabled-priority check, profile discovery, `RecurringScheduleAttribute` discovery) and
    `ConnectionOptionsBinderFactory`.
  - **`ReflectionTypeLoadException.Types` minus nulls**, which keeps the loadable types: `JobMasterRuntimeSingleton`
    and `SqlGenerator`.
- `JobMasterRuntime` runs its own scans at startup too (the same three as above), so even after the validator fix the
  AppDomain is still walked several times per startup.

**Design.**
- **Decided (Hugo):** a new internal static class, `TypeDiscovery`, that caches the discovered types. Every startup scan
  (the validator, `JobMasterRuntime`, IoC registration, `GetJobHandlerTypeFromId`) reads from it, so the AppDomain is
  walked once per process.
- **Cache per assembly, not one global list.** Use a `ConcurrentDictionary<Assembly, Type[]>` and re-read
  `AppDomain.CurrentDomain.GetAssemblies()` on every call. That list is cheap; `GetTypes()` is the expensive part.
  - A single frozen snapshot would miss assemblies loaded after the first scan: lazy-loaded handler assemblies, or a
    handler assembly first touched after `AddJobMaster` ran. Today's scans see those, because each scan re-reads the
    AppDomain.
  - The per-assembly cache sees them on the next call and still never calls `GetTypes()` twice on the same assembly.
  - An assembly's types never change once it's loaded, so the cache never needs invalidating.
- **Safe load:** catch `ReflectionTypeLoadException` and keep `ex.Types` without nulls, as `JobMasterRuntimeSingleton`
  does.
- **One assembly filter** (`!IsDynamic && Location != ""`), applied inside `TypeDiscovery`.
  - Check the call sites that don't filter today (`JobMasterRuntime`'s scans, `GetJobHandlerTypeFromId`). Applying
    the filter there excludes single-file/in-memory assemblies (empty `Location`), so confirm none of them depend on
    those.
- Optional typed shortcuts, e.g. `ConcreteTypesAssignableTo<T>()`, each cached once as well. These would cover the
  common `typeof(I).IsAssignableFrom(t) && !IsInterface && !IsAbstract` filter used at most call sites.
- Validator: keep the early return after the missing-`Config` check. Reading from the cache doesn't call `GetConfig`,
  so that ordering reason still holds.

**Decided (Hugo, 2026-10-30).**
- No logging of type-load failures: the loadable types are kept silently, like `JobMasterRuntimeSingleton` did.
- Typed shortcuts in this PR: `typeof(T).GetCachedConcreteTypesOf()` extension, `StaticRecurringSchedulesProfileTypes()`.
- Assembly filter: `!IsDynamic` only. The empty-`Location` filter is dropped, so single-file / in-memory assemblies are
  scanned everywhere (before, only some scans skipped them).

**Tasks.**
- [x] `TypeDiscovery` with a per-assembly cache and safe load. Unit tests:
  - `GetTypes()` called once per assembly across repeated calls;
  - an assembly loaded after the first call is picked up;
  - a `ReflectionTypeLoadException` keeps the loadable types; any other failure skips the assembly.
- [x] Move every listed call site onto it: the validator's four scans, `JobMasterRuntime`'s two left after item 13,
      `ClusterConfigBuilder`, `JobMasterIocRegistrationAttribute`, `GetJobHandlerTypeFromId`,
      `ConnectionOptionsBinderFactory`, `JobMasterRuntimeSingleton`, and `SqlGenerator`.
- [x] Do item 4 in the same PR: `GetJobHandlerTypeFromId`'s id→type map built from `TypeDiscovery`, covering the
      item 13 caller `JobUtil.GetHandlerPriority` (repeated misses don't rescan).
- [x] Internal ChangeLog. No docs change.

**Done when.** No startup call site calls `Assembly.GetTypes()` directly, each assembly's types are read once per
process, and the full unit suite is green.

---

## 15. Consistent cancel / reschedule method names

**Goal.** The cancel and reschedule methods on `IJobMasterScheduler` follow one naming rule. Found 2026-10-27 while
reviewing how cancel works (Hugo: "totally inconsistent").

**Current state.** All return `bool` ("did it happen"), but the `Try` prefix is random and the casing/parameter
names differ:

| Today | Job | Recurring |
|---|---|---|
| async | `CancelJobAsync(Guid jobId, ...)` | `TryCancelRecurringAsync(Guid id, ...)` |
| sync | `TryCancelJob(Guid id, ...)` | `CancelRecurring(Guid id, ...)` |
| reschedule | `ReScheduleAsync` / `ReSchedule(Guid jobId, DateTime scheduledAt, ...)` | – |

- `ReSchedule` uses a capital S, but "reschedule" is one word (`Reschedule`).
- Sibling bug: the sync `JobMasterSchedulerClusterAware.CancelJob` calls `masterJobsService.UpdateAsync(jobEntity)`
  without `await`, so it returns `true` before the write and a failure (e.g. version conflict) is lost. Fix in the
  same PR: call the sync `Update`.

**Design (proposed).** Drop `Try` everywhere: the `bool` is the whole result, there's no `out` value, and
`ReSchedule` already has no `Try`. In this codebase `Try` means "returns `T?`" (`TryGetX`).

```csharp
Task<bool> CancelJobAsync(Guid jobId, string? clusterId = null);
bool       CancelJob(Guid jobId, string? clusterId = null);
Task<bool> CancelRecurringAsync(Guid recurringScheduleId, string? clusterId = null);
bool       CancelRecurring(Guid recurringScheduleId, string? clusterId = null);
Task<bool> RescheduleAsync(Guid jobId, DateTime scheduledAt, string? clusterId = null);
bool       Reschedule(Guid jobId, DateTime scheduledAt, string? clusterId = null);
```

- **Decided (Hugo):** drop `Try`, and a plain rename: no `[Obsolete]` forwarders for the old names.
- Same names on the internal `IJobMasterSchedulerClusterAware`, and `JobRawModel.CanReSchedule` / `ReSchedule` →
  `CanReschedule` / `Reschedule` (internal, no shim needed).
- Parameter renames (`id` → `jobId` / `recurringScheduleId`) only break callers passing the name (`id: ...`);
  ChangeLog note.

**Open decisions.**
- Confirm "drop `Try`" vs "`Try` everywhere" (the latter would also rename `ReSchedule` → `TryReschedule`).
- Cancel doesn't check `ClusterMode.Archived`, reschedule does. Align (throw on archived for cancel too), or leave?

**Tasks.**
- [x] Rename on `IJobMasterScheduler` + `JobMasterScheduler`.
- [x] Rename on `IJobMasterSchedulerClusterAware` / `JobMasterSchedulerClusterAware` and the `JobRawModel` helpers.
- [x] Fix the sync `CancelJob` (sync `Update`).
- [x] Callers: `TargetTestScheduleApp`, `TargetTestRecurringApp`, unit tests. (`SampleWeb` already used
      `CancelRecurring`; `JobMaster.Api` doesn't call them.)
- [x] Unit tests: renamed test, sync `CancelJob` persists with sync `Update` before returning.
- [x] ChangeLog (breaking rename + parameter names, fixed sync `CancelJob`). Docs: item 10.
- [ ] Archived check for cancel (open decision above): not changed yet.

**Follow-up.** `CancelAsync` on the receipts is item 16 (separate PR, uses this item's naming).

**Done when.** One naming rule across all six methods, old names compile with an obsolete warning, and the unit suite
is green.

---

## 16. `CancelAsync` on receipts (follow-up to 15)

**Goal.** Cancel a just-scheduled job or recurring schedule from its receipt, including when it's still only on the
agent transport (`AcceptedOnTransport`), where today's cancel by id finds nothing on master and returns `false` while
the item runs moments later.

**Current state (2026-10-27).**
- Cancel by id is master-only: `GetAsync` → `TryToCancel` → version-checked `UpdateAsync`.
  - Not on master yet → `false`, the item runs.
  - Job `OnMaster` → cancelled, never assigned.
  - Job `InBucket` → cancelled; the worker's version-checked `PreEnqueue` update conflicts, reloads, sees `Cancelled`
    and drops it (`JobsExecutionEngine.PreEnqueue`).
  - Job due within ~50s (`TryToCancel`: 15s clock-skew padding + 30s `OnBoardingWindow` + 5s) → `false` even though
    found.
  - Queued / running / final → `false`.
- Cancel by id falls back to the default cluster when `clusterId` isn't passed; the receipt knows `ClusterId`.
- Nothing cancels an item that isn't on master yet. The unfinished `RecurringScheduleCancellingLock` check (never set)
  was removed in items 6+7.

**Design (decided, Hugo 2026-10-29).**
```csharp
public Task<bool> CancelAsync();   // on JobScheduleReceipt and RecurringScheduleReceipt
Task<bool> CancelJobAsync(JobScheduleReceipt receipt);              // IJobMasterScheduler, public
Task<bool> CancelRecurringAsync(RecurringScheduleReceipt receipt);  // IJobMasterScheduler, public
```
- `StoredOnMaster` → the normal cancel by id on the receipt's `ClusterId`.
- `AcceptedOnTransport` → **insert it already cancelled**: `BulkInsertIfNotExistsAsync` with the model in
  `Cancelled` / `Canceled` (final) status.
  - Inserted → done (`true`). The agent's later save hits the duplicate and drops its message (same path as item 7;
    the recurring duplicate fix covers schedules).
  - Not inserted (already on master) → the normal cancel by id.
  - Preferred over "confirm, then cancel": one write instead of two, and no window where the row is `OnMaster` and
    the assign runner could pick it up between the steps. Final rows are already inserted this way by the archive
    path.
- Returns `true` when the item is now cancelled; `false` when it's already queued/running/final, or inside the
  onboarding window.
- No archived check (Hugo, 2026-10-29): unlike confirm, the inserted row is final (`Cancelled` / `Canceled`), which
  an archived cluster may hold.

**Decided.**
- The ~50s window stays in the cancel-by-id fallback (Hugo, 2026-10-29). A job due now returns `true` only when the
  receipt cancel beats the agent's save. Relaxing it needs every unversioned update path checked first
  (`PreEnqueue` and the engine flush `BulkUpdateAsync(jobs)` are version-checked; the request-based
  `BulkUpdateAsync` isn't): separate investigation if ever needed.
- No batch version for now.
- A late cancel (after data retention purged a finished job) inserts a `Cancelled` row: fine, it's final and goes to
  archive / purge like any other (Hugo).

**Tasks.**
- [x] Receipt `CancelAsync` + public `IJobMasterScheduler.CancelJobAsync(receipt)` / `CancelRecurringAsync(receipt)`;
      cluster-aware `InsertCancelledJobAsync` / `InsertCancelledRecurringScheduleAsync`, then cancel by id.
- [x] Unit tests: `StoredOnMaster` → cancel by id; `AcceptedOnTransport` + not on master → inserted cancelled;
      already on master → cancel by id; archived still inserts it cancelled.
- [x] RepoConformance: already covered (final rows, inserted ids, existing untouched); added a `Cancelled` job to
      `BulkInsertIfNotExists_ShouldInsert_NewFinalizedJobs` (schedules already insert `Canceled`).
- [x] Unit test for the agent side: `JobSavePendingOperation` on a duplicate → `AlreadyExists`, nothing dispatched.
- [x] Scenario test (extended `ConfirmOnMasterTest/PostgresPure`, passed 2026-10-29): jobs due in 2 min cancelled
      from their receipts → all `true`, never run, `Cancelled` on master; jobs due now → those cancelled never run, the
      rest run once; recurring schedule → cancelled, never fires, `Canceled` on master. Also cancel by id once on master
      (`CancelFromId`, the app retries until the agent saved it): same outcomes. Postgres only: the logic is
      provider-independent and the repository parts are in RepoConformance.
- [x] ChangeLog (added). Docs: item 10.

**Done when.** A receipt cancel issued right after scheduling reliably prevents the run when it returns `true`, the
agent's late save never revives it, and unit + scenario tests are green.

---

## 17. Base64 ids in logs (messages + API `ReferenceId`)

**Status: noted for later** (Hugo, 2026-10-27), not scheduled yet.

**Goal.** An id copied from the dashboard (base64url, 22 chars) finds the matching logs, and log output shows ids in
the same format as the rest of the API/dashboard. Easier investigation from the dashboard (Hugo, 2026-10-27).

**Current state.** Three formats for the same Guid:

| Where | Format |
|---|---|
| API responses / dashboard URLs (`ApiJobModel.Id`, `SourceId`, `ApiJobExecution.JobId`, recurring `Id`, `ApiLogItem.Id`) | base64url (`ToBase64()`) |
| Stored log `ReferenceId` (`JobMasterLoggerExtensions` Guid overloads) | `"N"` (32 hex, no dashes) |
| Ids interpolated in log message text (`$"JobId={job.Id}"`) | `"D"` (with dashes) |

- `ApiLogItem.ReferenceId` is returned raw (`"N"`), unlike every other id in the API.
- API input already accepts any format: `ReferenceGuid` → `ParseFlexible()` → `"N"`. The dashboard queries with the
  base64 id (`ReferenceGuid: jid`); the dashboard doesn't display `referenceId` yet.
- Keyword search on the message text can't find a base64 id pasted from the dashboard (messages hold `"D"`).
- The archive / migrate / cleanup runners query job-execution logs by `ToString("N")`.
- `GuidBase64Extensions` (`ToBase64`, `ParseFlexible`) is internal to `JobMaster.Api`.

**Design (proposed).**
1. **Core helpers:** move `ToBase64` / `ParseFlexible` into `JobMaster` (internal + `InternalsVisibleTo`, or public if
   users should format ids the same way), so the SDK can use them. `JobMaster.Api` uses the core ones.
2. **Log message text:** job, recurring-schedule and execution ids in log messages are formatted with `ToBase64()`
   from now on. Only log messages: lock keys, document ids, SQL, exception messages used for control flow stay as
   they are. Mechanical change across the logging call sites (`JobsExecutionEngine`, `WorkerClusterOperations`,
   `JobSavePendingOperation`, runners, NATS runners, …).
3. **`ReferenceId` at the API boundary:** `ApiLogItem.FromDomain` returns `ReferenceId` as base64 when it parses as a
   Guid; non-Guid references (cluster, bucket, worker ids) unchanged. Storage stays `"N"`: no data migration, old rows
   keep working, the runners' `"N"` lookups are untouched, and `ReferenceGuid` input already accepts base64.

**Rejected:** storing `ReferenceId` as base64 too. One format end to end, but needs a migration (or dual-format
queries) for existing rows and changes to every runner that queries by `"N"`, while the API is the only place users
see it.

**Open decisions.**
- **Keyword search for old logs (optional):** when the keyword parses as a Guid in any format, also match the other
  two forms (`N`, `D`, base64), so a dashboard id still finds messages written before this change. Worth it, or accept
  that only new logs match?
- Core helpers internal or public?
- Exceptions whose message includes an id (e.g. Confirm's "could not be saved on master: {ids}"): base64 too, for the
  same reason?

**Tasks.**
- [ ] Move `GuidBase64Extensions` to core; `JobMaster.Api` uses it.
- [ ] Log messages: job / schedule / execution ids via `ToBase64()`.
- [ ] `ApiLogItem.ReferenceId` → base64 for Guid references. Update its XML doc.
- [ ] (If decided) keyword search matches all three Guid forms.
- [ ] Tests: `ApiLogItem.FromDomain` (Guid → base64, non-Guid unchanged); API test querying logs with the returned
      `ReferenceId`; helper round-trip tests.
- [ ] ChangeLog: API `ReferenceId` format change + log message id format.
- [ ] Dashboard: if it starts showing `referenceId`, link it to the job/schedule page (ids now match).

**Done when.** Ids in log messages and `ApiLogItem.ReferenceId` match the base64 ids the dashboard shows, existing
logs are still found by `ReferenceGuid`, and tests are green.

---

## 18. Archived safety net: strict mode flow + stray sweeper

**Goal.** A job or recurring schedule that lands on a cluster after it became Archived is never silently lost: it's
moved to the active cluster and runs there, or, when that's impossible, it's logged as Critical.

**Current state (2026-10-28).**
- Every scheduling path in `JobMasterSchedulerClusterAware` (`ConfirmOnMasterAsync` included) checks `GetFresh()` for
  `Archived`, then writes. The check isn't atomic with the write, so a cluster archived in between can always get a
  held job (see reminder "The archived check before a master write can't close the archived race"). No ordering of
  the check fixes it.
- An archived cluster runs only heartbeat + cleanup runners (`LoadArchivedRunnersAsync`): no buckets, no agent
  connections, no execution. A stray held job is stuck forever, and the caller was told it succeeded.
- Startup refuses to save `Archived` while unfinished jobs/schedules exist (`JobMasterRuntime`, before
  `masterConfigService.Save`), but never looks at the stored mode: an idle Active cluster can go straight to Archived.
- `TargetActiveClusterId` survives Migrating → Archived only by accident: line 165 keeps the stored value when the
  config has none, and the line 441 validation checks the configured definition, not the stored model.

**Design (proposed).**
1. **Strict flow.** Allowed: a new cluster (nothing stored) starts as Archived; Migrating → Archived; Archived →
   Archived (restart). Active → Archived throws ("switch to Migrating (`MigrateTo`) first"). Checked with the existing
   non-final check, before `Save`, so a refused transition leaves master untouched.
2. **Keep the target.** On Migrating → Archived the stored `TargetActiveClusterId` is kept on purpose (comment +
   test). Configuring `MigrateTo` on an archived cluster stays invalid; only the stored value carries over. With (1),
   every archived cluster that ever accepted writes has a target.
3. **Sweeper.** `LoadArchivedRunnersAsync` also starts `MigrateJobsRunner` and `MigrateRecurringSchedulesRunner`:
   - target stored and reachable → strays (held jobs, active schedules) move there and run normally;
   - no target (started as Archived) or target not registered in the archive process → Critical log per stray;
   - other unfinished statuses (shouldn't happen, no buckets) → Critical log.
   Reuses the existing "nothing else advances an `OnMaster` job, so migrate it" mechanism; the archived cluster never
   executes anything.

**Rejected.**
- Running strays through the fallback bucket on the archived cluster: needs agent connections, buckets and an
  execution worker, all forbidden on Archived, and archive deployments usually don't have the handlers.
- Per-job `jm-migrate-to-cluster-id` metadata stamped while Migrating: a write per job on the hot path, carries nothing
  the cluster doesn't already know (one target per cluster), and is most likely missing on exactly the strays (writers
  that read the mode just before the switch, or a cached `Get()`).
- Checking the mode after the write too: with the sweeper the stray still runs, so an exception would mislead.

**Open decisions.**
- Archived → Active / Archived → Migrating: allowed today. Restrict too, or leave?
- Archive process must register the target cluster for the sweeper to move strays (same requirement as
  `MigrateJobsRunner`). Validate at startup (warn/throw when a target is stored but not registered), or only log per
  stray?
- Sweeper interval on archived clusters: same as Migrating, or slower (strays are rare)?

**Tasks.**
- [ ] `JobMasterRuntime`: read the stored mode before line 166 overwrites it; throw on Active → Archived.
- [ ] Comment at line 165: stored `TargetActiveClusterId` kept on purpose for archived strays.
- [ ] `LoadArchivedRunnersAsync`: start the migrate runners; Critical log when there's no target / unreachable / other
      unfinished status.
- [ ] Unit tests: Active → Archived throws; Migrating → Archived ok; new → Archived ok; Archived → Archived ok; target
      survives Migrating → Archived.
- [ ] Scenario test: Migrating → Archived, insert a held job (and an active schedule) straight onto the archived
      cluster → moved to the active cluster and runs.
- [ ] Update the reminder (fix lives here) and the ChangeLog (breaking: Active → Archived refused; added: safety net).
- [ ] Docs (item 10): mode flow Active → Migrating → Archived.

**Done when.** Active → Archived is refused, an archived cluster with a stored target moves strays to it and they run,
strays with no way out are logged as Critical, and unit + scenario tests are green.

---

## 19. Startup: validate everything before anything starts

**Goal.** Every configuration error fails `StartJobMasterRuntimeAsync` before anything has side effects: no worker
running, nothing written to the databases. All errors are reported together, in the one "failed validation" list.
Raised by Hugo 2026-10-30 ("all the validation, if possible, should be before the start"), after item 13 showed the
static-schedule priority check throws once the workers are already running.

**Current state (2026-10-30, `JobMasterRuntime.StartAsync`).** Order today:
1. `ValidateAsync` of every `IJobMasterRuntimeSetup` (aggregated list), then `PreValidation()` (throws on the first).
2. `OnBeforeStartAsync` (provisioning).
3. Per-cluster loop: save agent connections, cluster config, worker registrations (`CreateAsync`).
4. `worker.StartAsync()` for each worker.
5. `BootstrapStaticRecurringSchedules`: read profiles and `[RecurringSchedule]` attributes, save the schedules.

**A. Thrown after the workers are running (step 5).** All configuration errors:
- Static schedule priority disabled on its cluster (item 13 extended it to the handler's priority).
- Profile problems: missing `ProfileId`; invalid `ProfileId` / `ClusterId` (`StaticRecurringSchedulesProfileInfo.IsValid`);
  missing `Config(...)`; the profile's own `Config(...)` throwing.
- `[RecurringSchedule]` on a class that isn't an `IJobMasterHandler`.
- Definition errors from `RecurringScheduleDefinitionCollection.Add`: duplicate static schedule id on a cluster,
  `MaxNumberOfRetries` above the maximum, missing expression / expression type, invalid definition id or schedule id.
- Precedent: the unknown profile `ClusterId` used to fail here too; item 1 moved it to
  `DefaultRuntimeValidatorSetup.ValidateStaticProfileClusterIds`.

**B. Thrown before the workers start, but after side effects (step 3).** Earlier clusters in the loop (and
`OnBeforeStartAsync`) have already written:
- Agent connection fingerprint changed with `ProtectConnectionChanges` (already in `reminders.md`, "validation is still
  split", point 2).
- Archived cluster still holding unfinished jobs / schedules (needs a DB query).
- "Agent connection … not found" (step 3) and `CreateAsync`'s "Cluster … not found": look unreachable because
  `PreValidation` guarantees both; confirm, then remove or keep as asserts.

**Out of scope.** Infrastructure failures (DB / transport down) while starting workers one by one: earlier workers keep
running, no rollback. Not validation.

**Design (proposed).**
1. **Static schedules (A):** build the static definitions once, during validation: discover profiles + attribute
   handlers, invoke `Config`, collect every definition. Each error above becomes an entry in the validation list
   (a profile's `Config` throwing is caught and reported with the profile name). The bootstrap in step 5 reuses the
   built definitions to save them; it no longer reads or validates. Testable `internal static` helpers in
   `DefaultRuntimeValidatorSetup`, like the item 1 / item 13 ones.
2. **Per-cluster checks needing the DB (B):** a check-only pass over all clusters, after `ValidateAsync` /
   `PreValidation` and before `OnBeforeStartAsync` writes anything: fingerprint check, archived non-final check. The
   save loop then only saves. This is the split `reminders.md` point 2 describes.
3. **`PreValidation`:** collect instead of throwing on the first error, so one list covers everything (optional; settle
   below).

**Open decisions.**
- Can the DB checks (fingerprint, archived non-final) run before `OnBeforeStartAsync`? Provisioning may create the
  tables they read (first start on an empty DB). Options: run them after provisioning but before any save (provisioning
  is idempotent setup, not data), or skip a check when its table doesn't exist yet.
- `PreValidation` → aggregated list too, or keep throwing on the first (it's structural, usually one mistake)?
- Is invoking a profile's `Config` during validation safe (user code with side effects)? It already runs once at
  bootstrap; this only moves it earlier and runs it once.

**Tasks.**
- [ ] Static definitions built during validation; all A errors aggregated; bootstrap reuses them. Unit tests per error.
- [ ] Remove the checks from `BootstrapStaticRecurringSchedules` (priority, profile, attribute) once covered.
- [ ] Check-only pass for B before any save; save loop only saves. Tests: fingerprint change with protection leaves
      every cluster untouched; archived with unfinished data leaves master untouched.
- [ ] Confirm the two "not found" throws are unreachable; remove or document.
- [ ] Update `reminders.md` ("validation is still split" → fixed here) + internal ChangeLog. User ChangeLog: startup
      errors that used to surface after the workers started now fail before anything starts.

**Done when.** No configuration error can be thrown after a worker starts or after a database write, and the unit suite
plus an existing scenario run are green.

---

## 20. `JobMasterAssemblyDiscovery()` for unreferenced libraries

**Goal.** Handlers, static recurring profiles, `[RecurringSchedule]` handlers and definition configs that live in a
library the app never names in code are found. Raised by Hugo 2026-10-30 while reviewing item 14.

**Current state.** .NET loads an assembly only when code first uses one of its types; a project reference alone loads
nothing (and the compiler drops unused references from the metadata, so `GetReferencedAssemblies` doesn't list it).
Every JobMaster scan only sees loaded assemblies. Typical case: split publisher / worker, where the worker gets its
handlers from a `Billing.Handlers` library and never names a type from it.
- Handlers: the job fails with "handler not found" (loud).
- Profiles / `[RecurringSchedule]`: no schedules are created, no error (silent).
- Definition configs: not validated at startup.
- Handlers are registered in DI during `AddJobMasterCluster` (`ClusterConfigBuilder.Finish`), from the assemblies
  loaded at that moment; an assembly loaded later fails with "not registered in the DI container".

**Design (decided, Hugo 2026-10-30).**
- Builder, not per cluster (discovery is AppDomain-wide), following the `Containing<T>` convention (MediatR,
  FluentValidation):
  ```csharp
  services.JobMasterAssemblyDiscovery()
      .AddAssemblyContaining<BillingHandler>()
      .AddAssembly(assembly)
      .AddAssemblies(params Assembly[] assemblies);
  ```
  Naming the type loads the assembly; `TypeDiscovery` also keeps the explicit set so `GetLoadedAssemblies()` includes
  it (covers plugin assemblies).
- Order doesn't matter: `serviceProvider.GetJobHandler(...)` falls back to
  `ActivatorUtilities.CreateInstance(serviceProvider, handlerType)` when the type isn't registered. A user's own
  registration (custom lifetime / factory) still wins. The DI registration in `ClusterConfigBuilder` stays.
- Clearer "handler not found" message (engine + recurring planner): the handler's assembly may not be loaded, use
  `JobMasterAssemblyDiscovery().AddAssemblyContaining<T>()`.

**Tasks.**
- [ ] Builder + `IServiceCollection` extension; explicit assembly set in `TypeDiscovery`.
- [ ] `GetJobHandler` fallback via `ActivatorUtilities`; clearer not-found message.
- [ ] Tests: added assembly is discovered; unregistered handler is created through the fallback; registered handler
      is preferred.
- [ ] XML docs in user language; ChangeLog (Added) + internal ChangeLog; docs-site note for item 10.

**Done when.** A handler and a profile in a library the worker never names run once the library is added through the
builder, regardless of call order; unit suite green.

---

## 21. Load provider assemblies by repo type (bug fix)

**Goal.** A cluster configured only through JSON (`ConfigFromJson`, `"RepoType": "Postgres"`) works with just the
provider package installed.

**Current state.** With JSON the provider is only a string, so nothing names a type from `JobMaster.Postgres` and the
assembly is never loaded. `JobMasterIocRegistrationAttribute.RegisterProviderExtensionsForMaster/Agent` then finds no
`[JobMasterIocRegistration]` class and registers nothing, silently. `ConnectionOptionsBinderFactory.Create` already
throws "Ensure the provider assembly is loaded" for `connectionOptions`. The fluent `UsePostgres(...)` style is fine
(it names a provider type, intentionally). Same gap for the internal `ClusterConnString(conn, repoType)` /
`AddAgentConnectionConfig(..., repoType, ...)` paths.

**Design.**
- Where a repo type string enters, load the known first-party assembly by name (`JobMaster.Postgres`,
  `JobMaster.MySql`, `JobMaster.SqlServer`, `JobMaster.RavenDb`, `JobMaster.NatsJetStream`) before the scan.
- Throw when no `[JobMasterIocRegistration]` matches the repo type after that (also catches typos in JSON):
  "repo type 'X' not found — install `JobMaster.X`".
- Separate commit from item 20 (Hugo).

**Open decision.** Recurrence providers (Cronos / NCrontab) have the same shape: a stored schedule only carries the
type id, and a process that never calls `AddJobMasterCronos()` fails with `Unknown recurrence type: Cronos` (loud).
Leaning: leave it (the explicit call is the intended pattern), only extend the error message to name the package and
the call.

**Tasks.**
- [ ] Repo type → assembly name load + "no registration" error. Unit test with a JSON-only config.
- [ ] ChangeLog (Fixed) + internal ChangeLog.

---

## 22. Lambda adapter: reserved definition-id prefix (idea)

**Goal.** A separate project lets users schedule static-method lambdas (Hangfire-style) to ease migration. Every job
needs a definition id, so lambda jobs need ids that never collide with user handlers. Idea only (Hugo 2026-10-30);
not part of the item 14 PR, scope TBD.

**Design notes.**
- Per-method id with a reserved prefix, e.g. `jm-lambda:Billing.Invoices.Send(System.Guid)`: dashboard / stats show
  which method ran, and a later move to a real handler can reuse a meaningful id.
- `JobDefinitionIdResolver.GetJobHandlerTypeFromId` checks reserved prefixes first and returns the adapter handler
  type; the adapter reads method + arguments from the job data. The adapter project registers its prefix + handler
  type through a small hook (internal + `InternalsVisibleTo`, like providers, or a minimal SDK API).

**Open decisions.**
- Prefix format: a character that can't be in a .NET `FullName` (e.g. `:`) so the `FullName` fallback never collides;
  consistent with `hybrid:` (item 8).
- Enforce the reservation: the startup validator rejects user `[JobMasterDefinitionId]` / `JobDefinitionConfig` ids
  starting with a reserved prefix.
- Per-job settings (priority, cluster, lane, timeout, retries) come from the call's optional parameters or defaults,
  not handler attributes; item 13's disabled-priority check must account for it.
- Security: invoking a method named in stored job data means anyone who can write to the master DB can call arbitrary
  static methods. Allow-list the types / assemblies registered with the adapter.
- Signature drift (method renamed / parameters changed while jobs are queued): fail with a clear "method not found".
