# Plan 0.0.12 Alpha

Original notes: [Plan0.0.12Alpha.original.md](Plan0.0.12Alpha.original.md).

Each item is meant to be picked up on its own, in order. Every item has: goal, current state, design,
open decisions (settle before coding), tasks, and done criteria.

**Delivery:** one PR per item, merged into the `0.0.12-alpha` branch. No rush — the version can take ~1 month.

| #  | Item                                                  | Size  | Breaking | Depends on |
|----|-------------------------------------------------------|-------|----------|------------|
| 1  | Cluster id via attribute / `JobDefinitionConfig`      | S     | No       | –          |
| 2  | NaturalCron upgrade + string overloads                | S     | No       | –          |
| 3  | Cronos/NCrontab colliding method names                | S     | No (obsolete) | 2     |
| 4  | `GetJobHandlerTypeFromId` single-scan cache           | XS    | No       | –          |
| 5  | Bootstrap-plumbing public/internal audit              | XS    | Maybe    | –          |
| 6  | `JobScheduleReceipt` scheduler contract               | M     | **Yes**  | 1          |
| 7  | Master flush safety net (`ConfirmOnMasterAsync`)      | M/L   | No       | 6          |
| 8  | Hybrid agent connection                               | L     | No       | own sub-plan file first |
| 9  | Reminders cleanup                                     | XS    | –        | 1–8        |
| 10 | Docs update (jobmaster-doc)                           | M     | –        | 1–9        |
| 11 | Docs restructure + `presenting.md`                    | M     | –        | 10         |

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
- New `JobDefinitionConfig.ClusterId` (optional ctor param, last position so existing calls compile).
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
- `CancelJob`/`ReSchedule`/`CancelRecurring` still take `clusterId` and default otherwise — a job scheduled
  to an attribute cluster can't be cancelled without passing the id. Accept + document, or out of scope?
  (Item 6's result exposes `Context.ClusterId`, which helps.)

**Tasks.**
- [ ] Attribute + `JobDefinitionConfig.ClusterId` + `ApplyOverrides` carries it through.
- [ ] `JobUtil.GetClusterId`; wire into `JobMasterScheduler` (all `Once*`/`Recurring*`, sync + async, both families).
- [ ] Static recurring: `RecurringScheduleDefinitionCollection` respects handler attribute per decided precedence.
- [ ] Validator: unknown cluster id + attribute-family mixing.
- [ ] Unit tests: precedence matrix (explicit / definition config / attribute / default) for jobs, dynamic
      recurring, static recurring, and the Advanced `TDefinition` overloads; validator failures.

**Done when.** Precedence matrix tests green; full unit suite green.

---

## 2. NaturalCron upgrade

**Goal.** Move to the newest NaturalCron on NuGet (fix from https://github.com/hugoj0s3/NaturalCron/pull/8).

**Current state.** `JobMaster.csproj` references `NaturalCron 1.0.1`. Usage is confined to
`src/JobMaster/RecurrenceExpressions/NaturalCron/*` and `NaturalCronScheduleAttribute`.

**Tasks.**
- [ ] Read PR #8 + NaturalCron changelog; note any API/behavior changes (parse errors, `TryParse` shape,
      next-occurrence semantics).
- [ ] Bump version; build; adjust `NaturalCronExprCompiler`/`NaturalCronCompiledExpr` if the API moved.
- [ ] Add a regression unit test for the specific case PR #8 fixed.
- [ ] Existing `NaturalCronExprExtensions` methods take `NaturalCronExpr`, not `string`, so they don't collide
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
- [ ] Add prefixed methods, obsolete old ones, update samples/benchmarks to the new names.
- [ ] Unit test: a project referencing both providers compiles a call to each (or at least both prefixed
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
- [ ] Make `JobMasterScheduler` internal again; rebuild solution.
- [ ] Grep for other concrete classes that only exist for `BootstrapBlueprintDefinitions` / DI bootstrap and are
      public; make internal where nothing outside `InternalsVisibleTo` uses them.
- [ ] Note any public→internal change in ChangeLog (technically breaking).

---

## 6. Scheduler contract: `JobScheduleReceipt`

**Goal.** Tell the caller *where* the job landed: accepted by the transport (agent, pending save to master) or
already stored on master.

**Current state.** Every scheduling method returns `JobContext` / `RecurringScheduleContext`.
`JobMasterSchedulerClusterAware.Schedule*` returns `void`/`Task` and internally takes one of two paths:
- bucket found → `AssignSavePendingJobToBucket` + `agentJobsDispatcherService.AddSavePendingJob…`
  → **AcceptedOnTransport**
- `ClusterMode.Migrating` / no bucket / bucket without `AgentWorkerId` → `MarkAsHeldOnMaster` + master add
  → **StoredOnMaster**

**Design.**
```csharp
// AcceptedOnTransport: stored on the agent transport only — lost only if the transport is gone for good
//                      before the save to master (item 7's safety net closes most of that window).
// StoredOnMaster:      persisted on master.
public enum ScheduleReceiptStatus { AcceptedOnTransport, StoredOnMaster }

// Confirmed: master has the row.
// Unknown:   timed out or master error — it may still commit afterwards. Never "not on master": a failure
//            doesn't prove the row isn't there (e.g. network error after commit). Calling again is safe —
//            the write is insert-if-not-exists by job id, so a retry can't duplicate.
public enum MasterConfirmationResult { Confirmed, Unknown }

public sealed class JobScheduleReceipt
{
    public ScheduleReceiptStatus Status { get; }
    public string? BucketId { get; }        // null when StoredOnMaster
    public string? AgentWorkerId { get; }   // null when StoredOnMaster
    public JobContext Context { get; }

    // Forces the save to master and waits (see item 7).
    // Not recommended on the hot path — documented as such.
    public Task<MasterConfirmationResult> ConfirmOnMasterAsync(TimeSpan? timeout = null);
}

public sealed class RecurringScheduleReceipt { /* same shape, RecurringScheduleContext Context */ }
```
- Naming decided: "Receipt" (accepted, not necessarily on master yet); `AgentWorkerId` matches
  `BucketModel`/`JobRawModel`; no `CancellationToken` — `IJobMasterScheduler` has none anywhere, timeout only.
- `ConfirmOnMasterAsync` returns `MasterConfirmationResult`, not `bool` (feedback from Reddit thread): a `bool`
  `false` would read as "not on master" when it really means "unknown". The receipt stays immutable —
  `Status` keeps the value from scheduling time; the confirmation is a separate answer.
- Sync methods return `JobScheduleReceipt`; async return `Task<JobScheduleReceipt>` — same type for both.
- `IJobMasterSchedulerClusterAware.Schedule*` returns the status/bucket/worker info so `JobMasterScheduler`
  can build the receipt.
- Until item 7 lands, `ConfirmOnMasterAsync` on `StoredOnMaster` returns `Confirmed` immediately; on
  `AcceptedOnTransport` it needs item 7's mechanism — so ship 6 and 7 in the same release.

**Decided.** Hard break — no `[Obsolete]` shims for the old `JobContext`-returning methods. Documented as a
breaking change (ChangeLog + docs migration note: `var ctx = scheduler.OnceNow<T>()` → `….Context`).

**Open decisions.**
- `ConfirmOnMasterAsync` default timeout value.

**Tasks.**
- [ ] Types + XML docs.
- [ ] `IJobMasterScheduler`, `IJobMasterSchedulerAdvanced`, `JobMasterScheduler` (all overloads).
- [ ] `IJobMasterSchedulerClusterAware` returns path info.
- [ ] Extension classes: NaturalCron / Cronos / NCrontab.
- [ ] Callers: JobMaster.Api, samples, benchmarks, scenario-test apps.
- [ ] Unit tests: both statuses for jobs + recurring (Migrating mode, no bucket, normal bucket).
- [ ] ChangeLog breaking-change entry.

---

## 7. Flush into master as safety net

**Goal.** Close the durability gap where a job accepted on the transport is lost if the agent dies before the
normal agent→master sync. Supersedes the "Cluster-aware bulk persistence" idea in
[src/JobMaster/Reminders.md](src/JobMaster/Reminders.md).

**Design (in `JobMasterSchedulerClusterAware`).**
- Keep an in-memory `AcceptedJobs` buffer of private DTOs `{ JobRawModel, CreatedAt, ConfirmationRequested,
  TaskCompletionSource }` for every `AcceptedOnTransport` job. Same for recurring schedules (separate buffer/DTO).
- Background timer, adaptive:
  - normal: every 5 s, check `ShouldFlush()`;
  - if any `ConfirmationRequested` or the backlog is large: every 50 ms until drained.
```csharp
// Both thresholds derived from the retention floor so they can't drift apart (10 min today).
static readonly TimeSpan SoftFlushAge = JobMasterDefaults.MinDataRetentionTtl * 0.5;  // 5 min
static readonly TimeSpan MaxBufferAge = JobMasterDefaults.MinDataRetentionTtl * 0.75; // 7.5 min, hard deadline

bool ShouldFlush() =>
       AcceptedJobs.Any(x => x.ConfirmationRequested)
    || AcceptedJobs.Count(x => x.CreatedAt < now - SoftFlushAge) >= 100
    || AcceptedJobs.Any(x => x.CreatedAt < now - MaxBufferAge); // deadline → fast (50 ms) mode until drained

const int FlushBatchSize = 500;
// batch = order by ConfirmationRequested desc, CreatedAt asc; take FlushBatchSize
// BulkInsertIfNotExists(batch) on master; remove from buffer; complete TCSs
```
- `ConfirmOnMasterAsync` = set `ConfirmationRequested = true`, kick the timer, await the TCS (with timeout).

**Why `MaxBufferAge` = 7.5 min prevents resurrecting purged rows (verified in code).**
- Jobs are purged by `FinalizedAt <= now - DataRetentionTtl` (`SqlMasterJobsRepository.PurgeFinalizedAsync`);
  recurring schedules by `TerminatedAt <= now - DataRetentionTtl` (`PurgeTerminatedAsync`).
- `ClusterDefinition.SetDataRetentionTtl` throws for any positive TTL below `MinDataRetentionTtl` (10 min).
- `FinalizedAt`/`TerminatedAt` ≥ `CreatedAt`, so the earliest possible purge is `CreatedAt + 10 min`.
  Flushing every buffered item before `CreatedAt + 7.5 min` means insert-if-not-exists can never recreate a
  purged row — no tombstone check needed. The 2.5 min margin absorbs flush latency and normal clock skew
  (`CreatedAt` is the scheduler's clock; `FinalizedAt`/purge cutoff are other machines' clocks — document it).
- Same cap applies to the recurring-schedule buffer.

**Master unavailable at the deadline.** Keep retrying while the item is younger than `MinDataRetentionTtl`.
Past that, still insert but log a warning — a purge in that window requires master to have been up and
processing, which contradicts master being down. Never silently drop (that loses the job the safety net
exists to protect).

**Open decisions / risks — settle before coding.**
- **Double write cost.** Every accepted job eventually gets an insert-if-not-exists on master even though the
  normal sync already saved most of them. Need a bulk "insert where id not exists" per provider (Postgres
  `ON CONFLICT DO NOTHING`, SQL Server `MERGE`/`WHERE NOT EXISTS`, MySQL `INSERT IGNORE`, RavenDB
  conditional put). Measure on the benchmark before/after.
- **Never overwrite.** Insert-if-not-exists must not touch a row the normal sync already advanced
  (Assigned/Running/Succeeded…). Purge resurrection is handled by `MaxBufferAge` above.
- **Memory bound.** At benchmark rates (250k jobs) a 5–7.5 min buffer is large. Need a max size + behavior
  when full (force flush / backpressure).
- **Process crash** loses the buffer — this is a safety net for agent loss, not process loss. Document.
- **Shutdown.** Graceful stop must drain the buffer to master.
- Where the cluster-mode switch to Migrating/Archived leaves buffered jobs (flush immediately?).

**Tasks.**
- [ ] Repository: bulk insert-if-not-exists for master jobs + recurring schedules (all providers) +
      RepoConformance tests.
- [ ] Buffer + timer + `ConfirmOnMasterAsync` wiring for jobs and recurring.
- [ ] Shutdown drain.
- [ ] Unit tests: `ShouldFlush` thresholds, ordering, batch size, confirm path, timeout.
- [ ] Unit test guarding the invariant: `SoftFlushAge < MaxBufferAge < JobMasterDefaults.MinDataRetentionTtl`.
- [ ] Unit test: master down past the deadline → keeps retrying, warns after `MinDataRetentionTtl`, never drops.
- [ ] Docs: clock-skew note + "safety net for agent loss, not process crash".
- [ ] Scenario test: kill agent transport after accept → job still reaches master and executes.
- [ ] Benchmark comparison (PostgresPure + PostgresNats) to quantify overhead.

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
hybrid:<transport><SEP><execution>
RepoType:    hybrid:NatsJetStream+RavenDb
Fingerprint: hybrid:<nats-fp>+<ravendb-fp>
```
- Role order is fixed (transport first), so swapping roles changes both strings → `ProtectConnectionChanges`
  catches it. Changing either underlying connection also changes the hybrid fingerprint.
- `SEP` (candidate `+`, alternative `||`) and the `hybrid:` prefix become **reserved**. Hard validation, with
  explicit throws:
  - any `IAgentFingerprintResolver` returning a fingerprint containing `SEP` or starting with `hybrid:` → throw
    (today all resolvers return GUIDs: Sql `ToString()`, NATS/RavenDB `ToString("N")`, so nothing breaks);
  - any provider `RepositoryTypeId` containing `SEP`/`:` → throw at registration
    (current ids: `Postgres`, `MySql`, `SqlServer`, `NatsJetStream`, `RavenDb` — all fine).
- Still to pick: `+` vs `||`. `+` reads better in logs/dashboard; `||` is less likely to appear in a future
  provider id. Either works once reserved.

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
- [ ] Pick the separator (`+` vs `||`).
- [ ] Settle the open decisions above; split the work into PRs in the sub-plan.

---

## 9. Reminders cleanup

There are two reminder files: [ChangeLog/reminders.md](ChangeLog/reminders.md) and
[src/JobMaster/Reminders.md](src/JobMaster/Reminders.md).

- [ ] Remove items finished by this release (3, 4, 5, hybrid sketch → replaced by item 8, cluster-aware bulk
      persistence → replaced by item 7).
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
