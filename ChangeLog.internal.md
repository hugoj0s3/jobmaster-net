# ChangeLog

> **Audience: framework contributors.**
> Documents implementation details, class-level changes, architectural decisions, and bug root causes for each release.
> This file will be kept updated until the stable version, at which point [ChangeLog.md](ChangeLog.md) will be populated from it as the user-facing release notes.

### 0.0.10-alpha
#### Added
- **Reserved `master-fallback-agent-conn` agent connection** (`JobMasterConstants`, `JobMasterClusterConnectionConfig`): new `AddMasterFallbackAgentConnectionString()` lazily registers a connection that reuses the cluster's own connection string/repository type, the same pattern already used for the standalone connection (`AddStandaloneAgentConnectionString`). Resolved automatically via `TryGetAgentConnectionConfig` and reserved against use by real agents in `JobMasterRuntime.PreValidation`. This is the connection the "fallback bucket" mechanism now uses instead of the Coordinator's own connection — see below.
- **`AgentWorkerModeExtensions.Is(AgentWorkerMode)`** (`JobMaster.Abstractions.Models`): `Full` satisfies checks for `Coordinator` (and any other mode) since it also runs those runners. Replaces the ad-hoc `mode == Coordinator || mode == Full` comparisons in `WorkerClusterOperations.CountActiveCoordinatorWorkersAsync` and the new bootstrap-time fallback-connection registration in `JobMasterRuntime`.

#### Changed
- **Fallback bucket now backed by the master DB, not an in-memory queue** (`AssignJobsToBucketsRunner`): previously, when no real bucket was available past `NoBucketFallbackThreshold`, a "fallback bucket" was created using the Coordinator's own `AgentConnectionId`, but its jobs were pushed into a private in-process list (`FallbackBucketJobsOnboardingSource`) — the agent-side `CreateBucketAsync`/`HasJobsAsync`/`DestroyBucketAsync` calls that bucket creation/destruction triggered were therefore dead work against a repository nothing ever read from. Fallback buckets are now created against the reserved `master-fallback-agent-conn` connection and dispatched through the exact same path as regular buckets (`StandardBucketJobsOnboardingSource`, `DispatchJobToBucketAsync`), making fallback jobs durable across a Coordinator restart. `FallbackBucketJobsOnboardingSource` is deleted.
- **Fallback connection heartbeat tied to fallback bucket lifetime** (`AssignJobsToBucketsRunner.OnTickAsync`): heartbeats the reserved connection only while `fallbackBucket` is non-null. It's expected to show "dead" in the dashboard whenever no fallback bucket is active — a good sign the fallback path isn't being used.
- **Reserved connections exempt from dead-connection cleanup** (`CleanupDeadAgentConnectionsRunner`): `standalone-agent-conn` and `master-fallback-agent-conn` are now excluded from the deletion sweep regardless of heartbeat status, since both are expected to sit dead for long stretches by design.

#### Fixed
- **Orphaned fallback buckets when the owning Coordinator dies** (`AssignedLostBucketsRunner`, `MasterBucketsService`, `DestroyReadyToDeleteBucketsRunner`): if the Coordinator (or Full worker) that created a fallback bucket died, `MarkBucketAsLostRunner` correctly flagged the bucket `Lost`, but `AssignedLostBucketsRunner` could never find a live worker to reassign it to — no real worker can ever own the reserved `master-fallback-agent-conn` connection (see `JobMasterRuntime.PreValidation`) — so the bucket stayed `Lost` forever, along with its agent-side `message_dispatcher`/`bucket_dispatcher` rows, which are only ever cleaned up via the `Draining`/`ReadyToDelete`/`DestroyBucketAsync` path. The affected *jobs* weren't lost (`HeldOnMasterDeadlineTimeoutJobsRunner` reclaims them independently once their deadline passes, regardless of bucket status), but the rows themselves leaked permanently in the master DB. Same bug also reachable via a graceful stop if jobs were still present when `DestroyReadyToDeleteBucketsRunner` next swept the bucket (it would revert it to `Lost` instead of destroying it). Fixed: `AssignedLostBucketsRunner` now sends a `Lost` Fallback bucket straight to `ReadyToDelete` instead of searching for an assignable worker; `MasterBucketsService.DestroyAsync` and `DestroyReadyToDeleteBucketsRunner` now skip the "has jobs" guard for Fallback buckets and force-destroy them regardless — safe because fallback buckets never carry save-pending jobs, only jobs reserved for execution, which recover independently via the deadline runner. Also closed a related gap where a `WorkerDefinition.AgentConnectionName` could be set to the literal reserved string, bypassing the existing `AgentConnections`-only reservation check — `JobMasterRuntime.PreValidation` now rejects that too.

- **Worker could be assigned the reserved standalone connection in a non-standalone cluster** (`JobMasterRuntime.PreValidation`): `JobMasterClusterConnectionConfig.TryGetAgentConnectionConfig`'s lazy resolution for `standalone-agent-conn` has no `IsStandalone` guard — it purely pattern-matches on name and reuses the cluster's own connection string/repo type. A non-standalone cluster's `AddWorker(name, agentConnectionName)` accepts an arbitrary string, and `PreValidation` only checked `AgentConnections` (registered connections) against the reserved name, not `Workers`, so a worker could be pointed at `standalone-agent-conn` by mistake and silently resolve. Fixed with the same pattern already used for `master-fallback-agent-conn`: `PreValidation` now rejects any non-standalone worker whose `AgentConnectionName` equals `standalone-agent-conn`. (The reverse — a standalone-cluster worker pointing elsewhere — was already structurally impossible: `ClusterStandaloneConfigBuilder.AddWorker` has no connection-name parameter at all.)

#### Changed
- **`AgentConnectionId` is nullable for Coordinator workers** (`AgentWorkerModel`, `JobMasterBackgroundAgentWorker`, `IJobMasterBackgroundAgentWorker`): a Coordinator never creates/owns a bucket under its own connection (the fallback bucket it may create uses the separate reserved `master-fallback-agent-conn`, not its own) and is never eligible to claim a bucket for draining (`AssignedLostBucketsRunner` only selects `Drain`/`Full` workers) — so it has no functional need for an agent connection of its own, and now isn't *allowed* to have one. `JobMasterBackgroundAgentWorker.CreateAsync` enforces this as a hard rule: `Mode == Coordinator` with an `AgentConnectionName` configured throws; any other mode without one resolving throws (existing behavior, unchanged). `MasterAgentWorkersService.RegisterWorkerAsync`/`CreateValidatedWorkerAsync`/`StopGracefulWorkerAsync` and the private `AgentWorkerRecord.ToModel` now treat an empty persisted connection id as `null` instead of throwing via the single-string `AgentConnectionId` constructor. `KeepAliveAgentConnectionRunner` is no longer started for Coordinator workers in `JobMasterBackgroundAgentWorker.StartAsync` (previously started unconditionally for every mode). All Full/Execution/Drain-only call sites that read `AgentConnectionId` (bucket runner factories, drain runners, save-pending runners) are guaranteed non-null by the mode split and use `!` accordingly; `AssignedLostBucketsRunner`'s worker-connection match was reordered to filter by `Mode` (Drain/Full) before comparing `AgentConnectionId`, since a Coordinator can now appear in the alive-workers list with a null connection. API surface (`ApiAgentWorker.AgentConnectionId`/`AgentConnectionName`, `WorkersEndpoints` filtering) updated to match.

---

### 0.0.9-alpha / Dashboard 0.0.2-alpha
#### Fixed

- **`x-jobmaster-doc` extension value** (`JobMaster.Api`, `JobMaster.Dashboard`): The Swagger document filter wrote `"jobmaster"` (the doc name) as the `x-jobmaster-doc` value, but the dashboard seeder validated against `JobMasterApiNamespaceKey.Key.ToString()` (`"JobMaster.Api.627b34633149493c9f293298ab209809"`). The two values never matched, so the seeder's identity guard always threw and no auth schemes or cluster IDs were ever applied. Fixed by writing `JobMasterApiNamespaceKey.Key.ToString()` in `JobMasterApiSwaggerSupport.ConfigureServices`.

- **`x-jobmaster-clusters` location** (`JobMaster.Api`, `JobMaster.Dashboard`): Cluster IDs were written to `swaggerDoc.Info.Extensions` but the seeder read them from `root["x-jobmaster-clusters"]` (document root). As a result the cluster list was never populated. Fixed by writing to `swaggerDoc.Extensions` (root) in `JobMasterApiSecurityDocumentFilter.ApplyClusterIds`.

- **`x-jobmaster-clusters` format** (`JobMaster.Dashboard`): The seeder assumed each array item was a JSON object with an `"id"` property, but the API writes plain string items. Fixed in `OpenApiJsonConfigSeeder.ApplyClusters` to handle both `JsonValueKind.String` and object items with an `"id"` property.

- **Dashboard seeder startup timing** (`JobMaster.Dashboard`): On first request the OpenAPI endpoint could still be initializing, causing `SeedAsync` to fail immediately. Fixed with a two-pass retry: immediate attempt, then a 1.5 s delay before the second attempt across the candidate URL list.

- **Worker mode counters swapped** (`dashboard`): In the dashboard metrics page, the `executionMode` counter filtered on value `1` (which is `Full`, not `Execution`) and `fullMode` filtered on value `2` (which is `Execution`, not `Full`). The counters displayed the wrong worker counts. Fixed by importing the `WorkerMode` enum and replacing magic numbers with `WorkerMode.Execution`, `WorkerMode.Full`, and `WorkerMode.Drain` constants.

- **`jobmaster-config.json` returns 500 instead of 404** (`JobMaster.Dashboard`): When the seeder's `SeedAsync` threw (e.g. the OpenAPI endpoint returned 404), the exception propagated as an unhandled 500. Fixed in `DashboardConfigEndpoints`: the endpoint now catches `HttpRequestException` with `StatusCode == NotFound` and returns `Results.NotFound()`; all other exceptions still propagate to produce a 500.

---

### 0.0.7-alpha
#### Added
- **Swagger & Sdk XML Documentation on API & Core Abstractions**: Added extensive XML `<summary>` comments to all public models, properties, authentication interfaces (such as `IJobMasterUserPwdAuthProvider`, `IJobMasterJwtBearerAuthProvider`, and configuration selectors), and core Sdk interfaces (like `IJobMasterScheduler`, `IJobHandler`, and DI configuration builders) across the entire framework to enable a rich developer/IntelliSense experience and auto-generated OpenAPI documentation.

- **Standardized Pagination & Sorting on API DTOs**: Standardized pagination (`CountLimit`, `Offset`) and sorting (`SortBy` of type `ApiSortByCriteria`) properties across all API query criteria models (`ApiAgentConnectionCriteria`, `ApiAgentWorkerCriteria`, `ApiLogItemQueryCriteria`, etc.).

- **`Onboarded` job status** (`= 10`, between `InBucket` and `Queued`): marks a job that
  has been accepted into a bucket's onboarding buffer and had its `ProcessDeadline` set to
  `UtcNow` as an instant recovery signal — if the bucket goes Lost, the deadline runner
  picks it up immediately without waiting for a natural expiry.

- **Type-Safe `BulkJobUpdateRequest` Pattern**: Introduced the `BulkJobUpdateRequest` and `BulkJobUpdateProperty` models, enabling type-safe LINQ-expression-driven property updates for batch database modifications (e.g. `Cancel` and `HeldOnMaster`), guaranteeing compile-time safety and safe refactoring.

- **Expanded Repository Conformance Test Suites**: Significantly expanded database integration and conformance testing (specifically `RepositoryJobsConformanceTests.cs`), introducing multi-case testing across 17 distinct query criteria filters (resolving a query visibility regression where locked jobs were incorrectly omitted) and verifying that the `ProbeForAcquireAsync` lookup counts only genuinely acquirable candidates.

- **Industrial-Grade Background Runtime Unit Test Harness**: Introduced a comprehensive unit test suite inside `Tests/JobMaster.UnitTests` covering all background operational layers. Includes rigorous validation of `OnBoardingControl` (deduplication, chronological sorting, and shutdown locks), `TaskQueueControl` (bounded capacities, promotional slots, and CPU backpressure), and dedicated mock-fixture testing (`RunnerFixture` and `RunnerFakes`) of all 20+ individual background runners (e.g. `AssignJobsToBucketsRunner`, `MarkBucketAsLostRunner`, etc.) to guarantee functional correctness.

- **`BulkJobUpdateRequest.HeldOnMaster(ids)` factory**: single-call shorthand for the
  standard HeldOnMaster bulk update (clears `Status`, `AgentConnectionId`, `AgentWorkerId`,
  `BucketId`, `ProcessDeadline`, `PartitionLockId`, `PartitionLockExpiresAt`). Adopted
  across all drain and deadline paths.

- **`JobMasterConstants.DefaultCacheEntryExpiry` (`8h`)**: single source of truth for
  sentinel-backed cache TTLs. Sentinel invalidation handles freshness; this is the
  safety-net expiry in case a notification is missed. Applied to `MasterBucketService`,
  `MasterAgentWorkersService`, `MasterClusterConfigurationService`, and
  `JobMasterInMemoryCache` default.

- **`ExcludeBucketIds` SQL support**: `SqlMasterJobsRepository.BuildWhere` now generates a
  `NOT IN` clause for `ExcludeBucketIds`, allowing runners to exclude jobs by bucket at the
  DB level.

- **NATS JetStream busy-retry**: when onboarding is full the runner NAKs with increasing
  delays (30s → 75s → 3 min) tracked via an in-memory counter (avoids `NumDelivered` noise
  from TooEarly redeliveries). After 3 retries the job is redirected to master.

- **`JobMasterDefaults` static class**: centralises all default configuration values that
  were previously scattered as magic numbers — `DefaultJobTimeout` (1 min),
  `TransientThreshold` (10 min), `MaxRetryCount` (3), `MaxMessageByteSize` (128 KB),
  `DataRetentionTtl` (30 days), and nested `Worker` defaults: `TransferBatchSize` (1000),
  `BucketBufferSize` (250), `BucketBufferLeadTime` (15 s), `ParallelismFactor` (1.0),
  `BucketQtyPerPriority` (1), `DefaultMode` (Full).

- **`MsgAckGuard.TryAckProgressAsync()`**: new method that sends a NATS `AckProgress`
  to reset `AckWait` without committing a final outcome — used by the per-message
  keep-alive loop in `NatsJetStreamRunnerBase`.

- **`MsgAckGuard` hourly cleanup timer**: background timer evicts entries older than 2 h
  from `FailureAttempts`, `BusyRetryCount`, and `LastUpdatedAt` dictionaries, preventing
  unbounded memory growth in long-running agents.

- **`IJobMasterSchedulerClusterAware.Schedule`/`ScheduleAsync` `notifyAgent` parameter**:
  new optional `bool notifyAgent = true` flag lets callers suppress the agent notification
  (useful for bulk/batch scheduling paths).

- **`JobMasterJobStatus` helpers**: new `GetPreExecutionBucketStatuses()` (InBucket,
  Onboarded, Queued) and `IsPreExecutionBucketStatus()` extension method.
  `JobMasterJobStatusUtil` changed from `public` to `internal`.

- **`UseNatsJetStream` multi-server overloads**: two new `ConfigExtensions` overloads accept
  a `string[]` of pre-built connection strings or a `(url, userName, password)[]` tuple array,
  joining them into a single cluster connection string. URL normalisation (`nats://` prefix,
  credentials embedding) is handled internally so callers pass plain host:port values.

- **`ApiJobExecution` enriched response**: three new fields — `AgentConnectionName`
  (display name of the agent connection), `HostId`, and `HostDisplayName` — are now
  included in every job execution record returned by the API.

- **`ApiJobQueryCriteria` new filters**: `HostId`, `BucketId`, `WorkerLane`,
  `AgentConnectionId`, and `WorkerId` filter parameters added to the jobs query endpoint.

- **`ApiRecurringScheduleModel.IsStaticIdle`**: new boolean field indicating the schedule
  is static (startup-defined) but not currently Active.

### Changes
- **`ApiLogItem` message truncation**: Added a `CutMessage()` utility to `ApiLogItem` as a payload-reduction strategy, truncating the log message to the first 100 characters when requested to reduce serialization overhead under high-volume logging.

- **Worker auto-name simplified**
  Auto-generated names are now `{hostname}-{timestampId}` (e.g. `myserver-3c1a8b2`).
  Explicit names follow the same pattern: `{workerName}-{timestampId}` (e.g. `payroll-01-3c1a8b2`).
  The `workerId` for explicit names is now derived from `workerName` instead of `hostId`, making it stable and predictable.
  See [WorkersConfiguration](docs/WorkersConfiguration.md) for naming guidance.

- **Deadline runner is now a safety net only**: `HeldOnMasterDeadlineTimeoutJobsRunner`
  excludes jobs whose bucket is Active or Completing via `ExcludeBucketIds`. The drain
  path inside the engine is the primary mechanism; the deadline runner only intervenes when
  a bucket is lost or the drain fails.

- **Unified `AllBuckets` cache**: `MasterBucketService` now uses a single sentinel-backed
  `AllBuckets` cache for both `Query`/`QueryAsync` (when `allowedDiscrepancy` is provided)
  and `SelectBucketForJob`. `NotifyChanges` is always called before the DB operation, and
  both `AllBuckets()` and `Bucket(id)` sentinel keys are notified on every mutation.
  `IMasterBucketsService.Query`/`QueryAsync` now accept an optional `allowedDiscrepancy`
  parameter.

- **`MasterAgentWorkersService`**: `NotifyChanges` moved before DB operations in all
  mutating methods; missing notifications added to both `Delete` variants.

- **`JobsExecutionEngine` refactor**: extracted `FlushAuthorizedJobsAsync`,
  `EnqueueJobsAsync`, and `PullPendingJobsAsync` from `PulseAsync`; moved `shouldSkip`
  check after the Completing drain block; removed stale `ExceedProcessDeadline` guard
  inside `ExecuteJobAsync` (with `ProcessDeadline = UtcNow` set by `Onboard()` it would
  drop every job); `FlushToMasterAsync` uses `BulkJobUpdateRequest.HeldOnMaster`.

- **`ManualDrainProcessingJobsRunner`**: uses `BulkJobUpdateRequest.HeldOnMaster`.

- **`IOnBoardingControl` cleanup**: removed unused `Contains`, `Push`, `Count`,
  `PruneDeadlinedItems`, and `GetNextDepartureTime`; renamed `PruneOldDepartureItems` →
  `PullPending`.

- **`ITaskQueueControl` cleanup**: removed `AbortTimeoutTasks()` — superseded by
  `CancelAfter(Timeout)` in `TaskQueueItem.Start()`.

- **`JobRawModel`**: added `Onboard()` method; renamed `Enqueued()` → `Enqueue()`.

- **`MsgAckGuard` thread-safety overhaul**: all ack/nak methods now acquire a
  `SemaphoreSlim(1,1)` before checking `Outcome`, preventing double-ack/nak races when two
  concurrent threads handle the same message. `messageId` is now stored as an instance field
  (was passed as a parameter to every call). `MsgAckGuard` now implements `IDisposable`.

- **`NatsJetStreamRunnerBase` per-message AckProgress keep-alive**: a background task
  started in `ProcessMessageAsync` periodically calls `TryAckProgressAsync()` at
  `CalcAckProgressKeepAliveInterval(BucketBufferLeadTime)` intervals. The task is cancelled
  and awaited in the `finally` block before the final ACK/NAK to avoid a stale progress
  signal after the message is finalised.

- **`NatsJetStreamJobsExecutionRunner` backpressure pulse signal**: a `volatile
  TaskCompletionSource<bool>` `pulseSignal` gates `ProcessPayloadAsync` when the engine is
  near capacity (or randomly at 5 %), creating light backpressure that prevents the NATS
  consumer from pulling faster than handlers can process.

- **`NatsJetStreamJobsExecutionRunner` new `OnBoardingResult` handling**:
  `MovedToMaster` (debug log + return), `Invalid` (warning log, marks job `HeldOnMaster`,
  persists), and `Busy` (delegates to `TryNakBusyAsync` with exhaustion escalation).

- **`NatsJetStreamConstants` centralised timing helpers**: `CalcAckWait`,
  `CalcAckProgressKeepAliveInterval`, and `CalcMessageLockDuration` replace duplicated
  inline calculations. New constants: `AckOperationTimeout` (5 s), `HeartbeatPublishInterval`
  (10 s), `ConsumerIdleHeartbeat` (5 s), `BusyRetryDelays` ([30 s, 75 s, 3 min]).
  `DefaultDbOperationThrottleLimitForAgent` lowered from 1000 → 250.

- **`NatsJetStreamConnector` AckWait refactor**: `ackWait` now calculated via
  `NatsJetStreamConstants.CalcAckWait(bucketBufferLeadTime)` instead of a local formula.

- **`JobsExecutionEngine` staging buffer + engine lock**: `TryOnBoardingJobAsync` now
  appends to a `jobsToFlush` list (guarded by `jobsToFlushLock`); `FlushToOnBoardingControlAsync`
  batch-persists them as `Onboarded` on each pulse. A `SemaphoreSlim engineLock` serialises
  all pulse work in `PulseAsync`. A `recentlyHeldOnMasterIdsGuard` (version-matched, expires
  after `ErrorHoldDuration`) prevents re-onboarding a job just moved to master, avoiding DB
  version conflicts. `Completing` buckets immediately redirect jobs to master at onboard time.
  `PreEnqueuedAsync` (async) changed to `PreEnqueue` (sync), removing an async dependency
  from the enqueue hot path.

- **`TaskQueueControl.ShutdownAsync` race fix**: snapshot of `Tasks` now taken inside a
  lock before iteration. Boolean condition in `while` changed from `||` to `&&`.
  New `GetRunningTimeouts()` returns all running task `Timeout` values.
  New `GetIds()` returns a snapshot of all tracked IDs (waiting + running).

- **`OnBoardingControl` API simplification**: `Push` no longer takes a `departureDeadline`
  parameter — deadline enforcement moved to the engine layer. `ForcePush` removed (folded
  into the staging buffer). `PruneDeadlinedItems` removed. New `GetIds()` method returns a
  snapshot of all tracked IDs.

- **Heartbeat publisher decoupled from data messages**: `NatsJetStreamRunnerBase` now
  tracks `lastHeartbeatPublishedAt` independently and fires on `HeartbeatPublishInterval`
  (10 s) regardless of data message throughput — previously a quiet stream could delay
  heartbeat publication.

- **GUID v7 for all entity IDs** (breaking schema change): jobs, recurring schedules,
  job executions, and distributed lock records now use time-ordered GUID v7
  instead of random v4 GUIDs. Time-ordered IDs eliminate index page splits on insert-heavy
  workloads. `JobMasterRandomUtil` was updated with a cryptographic `NewGuid7()` implementation, and all three SQL provider repositories (MySQL, Postgres, SQL Server) were updated to generate v7 IDs.

- **`JobMasterRandomUtil` allocation optimizations**: Integrated a thread-safe nested generic `EnumCache<T>` inside `JobMasterRandomUtil.GetEnum<T>()` to cache enum value arrays, bypassing expensive reflection allocations during mock data generation on hot paths.

- **Dedicated Log & Job Execution Tables (Database Decoupling)**: Completely decoupled logging and job execution tracking from the high-contention generic records database tables:
  - **New Database Tables**: Introduced the dedicated `log` and `job_execution` tables, featuring concrete columns for properties (e.g. `level`, `category`, `reference_id`, `started_at`, `finalized_at`, `host_id`, `outcome`) and optimized compound indexes (`idx_log_cluster_level_timestamp`, `idx_log_cluster_category`, `idx_job_execution_cluster_job_id`) to accelerate query throughput and audit trail reads.
  - **Decoupled Repositories**: Introduced the new `IMasterLogsRepository` and `SqlMasterLogsRepository` inside `JobMaster.SqlBase` (implemented across MySQL, PostgreSQL, and SQL Server) to manage the dedicated `log` table. `JobMasterLogger` was refactored to flush structured `LogItem` batches directly to this repository, discarding the heavy `LogPayload` and generic record transformations.
  - **Transactional Execution Bindings**: Migrated `job_execution` writes directly into the `SqlMasterJobsRepository.UpdateAsync` database pipeline. Saving a job state change and appending its execution history (upon processing startup or completion) now occurs inside a **single, secure database transaction**, ensuring full atomicity and eliminating version-conflict split-states.
  - **Service Consolidation**: Removed `MasterJobExecutionService` and `IMasterJobExecutionService` entirely; their query responsibilities were absorbed by `IMasterJobsService.QueryJobExecutionsAsync(guid)` and the underlying `IMasterJobsRepository`.

- **NATS key generation standardization**: Refactored `NatsJetStreamAgentFingerprintResolver` and NATS dispatcher repositories to use the centralized framework utility `JobMasterRandomUtil.NewGuid4()` instead of raw `Guid.NewGuid()`.

- **Redesign of `IMasterJobsRepository` contract (breaking change)**: Updated the master repository contract to completely replace legacy `Upsert` methods with `Update`/`UpdateAsync` (which support a single-transaction `JobExecution` payload to prevent split-state database bugs), replace hardcoded `BulkUpdateStatus` with the new type-safe `BulkUpdateAsync(BulkJobUpdateRequest)`, and add the `ProbeForAcquireAsync` lookup.

- **Centralization of SQL Base Repositories**: Refactored the provider-specific databases (PostgreSQL, MySQL, SQL Server) to inherit directly from centralized base classes inside `JobMaster.SqlBase` (such as `SqlMasterJobsRepository` and `SqlMasterLogsRepository`). This fully eliminated thousands of lines of duplicated query-builder and DTO mapping code from individual provider repositories, ensuring high maintainability.

- **`AssignJobsToBucketsRunner` probe mechanism**: a lightweight `JobProbeResult`
  (Count + MinNextPlanExecutionAt) query runs before the full dispatch scan, enabling a
  two-tier pattern — skip the expensive scan entirely when the count is zero or the earliest
  job is not yet due. `ProbeDiagnosticResult` captures timing for observability.
  `IMasterJobsRepository` and `IMasterJobsService` gained the corresponding probe methods.

### Fixed

- **Critical: `NatsJetStreamRunnerBase` NATS consumer restart loop**: `ListenMsgsAsync`
  is now wrapped in `while (!ct.IsCancellationRequested)` so `ConsumeAsync` is automatically
  restarted if the server closes the subscription (heartbeat expiry, server restart, etc.).
  `IdleHeartbeat = 5 s` added to `NatsJSConsumeOpts` to keep the pull subscription alive
  during idle periods. This was the root cause of the 63–81 lost jobs observed in
  `SchedulerTest(1000)` — see `project_nats_ackwait_root_cause.md` for the original analysis.

- **`NatsJetStreamRunnerBase` `OperationCanceledException` handling**: a cancelled
  `ConsumeAsync` that is not a user-requested shutdown now logs and continues the restart
  loop instead of propagating. `StopConsumptionTaskAsync` catches both `TimeoutException`
  and `OperationCanceledException when timeoutCts.IsCancellationRequested`.

- **`NatsJetStreamRunnerBase` bad heartbeat no longer kills the subscription**: a
  heartbeat signature mismatch now `continue`s the inner loop instead of `return`ing, so
  a single malformed heartbeat cannot tear down the entire consumer.

- **`NatsJetStreamConnector` .NET 6 compatibility**: `#if NET8_0_OR_GREATER` changed to
  `#if NET6_0_OR_GREATER` for the `IAsyncDisposable` code path — previously the connector
  would not dispose correctly on .NET 6 or .NET 7.

- **`AckProgress` CTS independence**: the per-message `AckProgress` cancellation token
  source is now independent of the consumer lifecycle token, fixing a potential issue where
  cancelling the consumer would also cancel an in-flight `AckProgressAsync` call.

- **`TaskQueueControl` `IsTaskDead` completion treated as dead**: `RanToCompletion` is
  now included alongside `Faulted`/`Cancelled` in the dead-task check, since
  `ListenMsgsAsync` should never exit normally.

- **`ComputePostponeDuration` missing `PostponeFactor` multiplier**: the average running
  job timeout was not being scaled by `PostponeFactor` before adding `MinPostponeDuration`,
  causing the postpone window to be shorter than intended and re-triggering jobs too soon.

### Renamed

- **`footprint` → `fingerprint`** (pervasive, breaking change): `IAgentFootprintResolver`
  → `IAgentFingerprintResolver`, `GiveYourFootprintAsync` → `GiveYourFingerprintAsync`,
  `NatsJetStreamAgentFootprintResolver` → `NatsJetStreamAgentFingerprintResolver`, NATS KV
  key suffix `agent_footprints` → `agent_fingerprints`, SQL table `agent_conn_footprint` →
  `agent_conn_fingerprint`, SQL column `footprint` → `fingerprint`.

- **`JobMasterLogSubjectType` → `JobMasterLogCategory`** (pervasive, breaking change):
  enum `ApiJobMasterLogSubjectType` deleted; new `ApiJobMasterLogCategory` added with the
  same integer values (Job=1…Api=7). `ApiLogItem.SubjectType` → `Category` (type updated),
  `ApiLogItem.SubjectId` → `ReferenceId`. Same renames applied to
  `ApiLogItemQueryCriteria` and all internal usages in runners and engine.