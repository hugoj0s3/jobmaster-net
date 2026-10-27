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
| 10 | Docs update (jobmaster-doc)                           | M     | –        | 1–9, 12, 13 |
| 11 | Docs restructure + `presenting.md`                    | M     | –        | 10         |
| 12 | Case-insensitive metadata/message-data keys + worker lanes | M | **Yes** | 1          |
| 13 | Disabled-priority validation scoped per cluster       | S/M   | Maybe (stricter recurring check) | 1 |
| 14 | Startup type scans: one pass + safe `GetTypes`        | S     | No       | 1 (13 adds a scan user) |

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
path (`JobsExecutionEngine`, `RecurringSchedulePlanner`).

**Tasks.**
- [ ] On first miss, build the full id→type map from one scan (via `GetJobDefinitionId`, same as
      `DefaultRuntimeValidatorSetup`); thread-safe publish (build then swap).
- [ ] Decide behavior for an id still missing after the full map exists (assemblies loaded later?) — rescan
      once, or treat as not found.
- [ ] Unit tests: N distinct types → 1 scan; unknown id → null.

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
    public JobContext GetJobContext();                      // built on demand from the model
    public Task ConfirmOnMasterAsync();                     // item 7
}

public sealed class RecurringScheduleReceipt { /* same shape, GetRecurringScheduleContext() */ }
```
- `JobMasterSchedulerClusterAware.Schedule*` returns `BucketModel?`: the bucket it was handed to, `null` = stored on
  master. `JobMasterScheduler` turns it into the receipt (`ToReceipt(rawModel, bucket)`).
- Receipt fields are captured in the constructor, so later changes to the model (item 7) don't change what the
  receipt shows. Hot-path cost: one receipt object per call, nothing else.
- Sync methods return the receipt, async return `Task<receipt>`; no `CancellationToken` (the scheduler has none).

**Decided.** Hard break, no `[Obsolete]` shims. ChangeLog + docs migration note:
`var ctx = scheduler.OnceNow<T>()` → `scheduler.OnceNow<T>().GetJobContext()`.

**Tasks.**
- [x] Types + XML docs.
- [x] `IJobMasterScheduler`, `IJobMasterSchedulerAdvanced`, `JobMasterScheduler` (all overloads).
- [x] `IJobMasterSchedulerClusterAware` returns the bucket.
- [x] Extension classes: NaturalCron / TimeSpan / Cronos / NCrontab.
- [x] Callers: benchmarks, scenario-test apps (JobMaster.Api and samples don't use the return value).
- [ ] Unit tests: both statuses for jobs + recurring (Migrating mode, no bucket, normal bucket).
- [ ] ChangeLog breaking-change entry.

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
  and the runner re-queued it forever; now treated as already saved, like the job path.
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
- [ ] Unit tests: StoredOnMaster skipped; AcceptedOnTransport marked held/Active and inserted; grouping across
      clusters; partitioning; recurring duplicate no longer re-queued.
- [ ] RepoConformance: `BulkInsertIfNotExistsAsync` with non-final (`OnMaster` / `Active`) rows, all providers.
- [ ] Scenario test: confirm right after scheduling → job reaches master and executes once.
- [ ] ChangeLog (added) + docs.

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

**Open decisions.**
- Dynamic recurring schedules with a handler-resolved priority:
  - fail at registration (`RecurringAsync`) by resolving the handler priority up front for the check, without storing
    it, so per-occurrence resolution is kept;
  - fail or warn when planned;
  - or keep the delay-then-fallback and only log a warning.
  - Registration-time failure is the most consistent with one-off jobs.
- Is a definition-only `IStaticJobDefinitionConfig` (publisher side, no handler) with a disabled priority on its own
  `ClusterId` also a startup error? It would match item 1's cluster-id validator scope.

**Tasks.**
- [ ] Per-cluster startup validation helper + unit tests:
  - Critical handler on the default cluster with Critical disabled only on cluster-b → OK.
  - The same handler with `[JobMasterClusterId("cluster-b")]` → error.
  - `JobDefinitionConfig` priority checked.
  - Inherited `[JobMasterPriority]` checked.
- [ ] Remove the old all-handlers loop from `PreValidation`.
- [ ] Static recurring: resolved-priority check per definition cluster.
- [ ] Dynamic recurring: implement the decided behavior + tests.
- [ ] Docs (`cluster-configuration.md` DisablePriority section, `job-configuration.md` priority warning) + ChangeLog.

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

**Open decisions.**
- Should a type-load failure be logged, so an assembly whose types silently partly disappear is still visible? The
  per-assembly cache would log it once per assembly.
- Typed shortcuts in this PR, or just the raw cached `GetTypes()`?

**Tasks.**
- [ ] `TypeDiscovery` with a per-assembly cache and safe load. Unit tests:
  - `GetTypes()` called once per assembly across repeated calls;
  - an assembly loaded after the first call is picked up;
  - a `ReflectionTypeLoadException` keeps the loadable types.
- [ ] Move every listed call site onto it: the validator's four scans, `JobMasterRuntime`'s three,
      `ClusterConfigBuilder`, `JobMasterIocRegistrationAttribute`, `GetJobHandlerTypeFromId`,
      `ConnectionOptionsBinderFactory`, `JobMasterRuntimeSingleton`, and `SqlGenerator` (`JobMaster.SqlBase` already
      has `InternalsVisibleTo`).
- [ ] Coordinate with item 4: `GetJobHandlerTypeFromId`'s id→type map can be built from `TypeDiscovery`.
- [ ] Internal ChangeLog. No docs change.

**Done when.** No startup call site calls `Assembly.GetTypes()` directly, each assembly's types are read once per
process, and the full unit suite is green.
