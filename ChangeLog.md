# ChangeLog

> **Audience: framework users.**
> Documents new features, breaking changes, and user-visible fixes for each release.
> For internal implementation details see [ChangeLog.internal.md](ChangeLog.internal.md).

---

## [Unreleased]

### Added

- **`ConfigFromJson` — full cluster config from JSON** — The entire cluster setup (cluster settings, agent connections, workers) can now be driven from a JSON file or string via `ConfigFromJson`, removing the need for code-level wiring. Provider-specific connection settings (e.g. NATS auth and TLS) are supported through a `connectionOptions` dictionary — `JobMaster.NatsJetStream` accepts auth keys (`username`, `password`, `token`, `credentialsFile`, `nkey`, `jwt`) and TLS keys (`tlsCertBundleFile`, `tlsCaFile`, `tlsMode`, etc.).

### Changed

- **`ReadIsolationLevel` removed** — The internal `ReadIsolationLevel` enum and all `ReadIsolationLevel` properties on query criteria classes have been removed. All database reads now use `READ COMMITTED`. SQL Server users are recommended to enable `READ_COMMITTED_SNAPSHOT` (RCSI) on their database to achieve equivalent non-blocking read behaviour without dirty reads.

- **`IClusterConfigSelector` method renames** — The `Cluster` prefix has been removed from cluster configuration methods (`DefaultJobTimeout`, `TransientThreshold`, `DefaultMaxRetryCount`, `MaxMessageByteSize`, `IanaTimeZoneId`, `Mode`). Old names still compile but are marked obsolete and will be removed in a future release. `ClusterId` is unchanged.

- **Coordinator workers no longer take an agent connection — and now must not have one** — A Coordinator only assigns jobs to buckets and manages their lifecycle; it never owns a bucket or claims one for draining, so it has no use for an agent connection of its own. Configuring an `AgentConnectionName` for a Coordinator worker now throws at startup instead of being silently accepted. Every other mode (`Full`, `Execution`, `Drain`) is unaffected and still requires one.

- **`IJobHandler` renamed to `IJobMasterHandler`** — Brings the interface you implement to define a job in line with the rest of the library's naming (`IJobMasterScheduler`, `IJobMasterLogger`, etc.). Existing code implementing `IJobHandler` keeps compiling as-is — it's now marked obsolete and will be removed in a future release, so there's no rush to migrate, but new handlers should implement `IJobMasterHandler` directly.

### Fixed

- **Coordinator fallback bucket durability** — When no bucket is available for a job for too long, the Coordinator's temporary "fallback bucket" now persists jobs to the master database (through a dedicated reserved connection) instead of an in-process queue, so they survive a Coordinator restart instead of being lost.

- **Orphaned fallback buckets after a Coordinator crash** — If the worker that created a fallback bucket died, the bucket could get stuck forever instead of being cleaned up, leaking rows in the master database (the jobs themselves were never at risk — only the bucket's own bookkeeping). Fallback buckets are now destroyed automatically once their owning worker is confirmed dead.

- **`ProtectConnectionChanges` now actually protects a connection** — Previously this setting was silently dropped and never persisted, so no agent connection could ever truly become protected regardless of configuration. It's now saved correctly. Dead connections with no buckets left are automatically cleaned up after 30 minutes regardless of this setting — `ProtectConnectionChanges` only affects whether a silently-changed connection is rejected at startup, not how long a dead connection lingers.

- **A recreated agent connection, worker, or host could be marked dead immediately** — If a connection, worker, or host was deleted and later recreated, its previous heartbeat history wasn't cleared, and could incorrectly make the brand-new record look already stale enough to be considered dead. All three now correctly treat a freshly (re)created record as alive.

- **Worker "alive" window standardized to 45 seconds** — Agent workers previously used a 30-second window with no allowance for clock drift between machines, while agent connections used 90 seconds and hosts used 45 seconds. All three now consistently use the same 45-second window (30s heartbeat threshold + 15s clock-skew allowance).

- **SQL agent connections: possible false-positive "fingerprint has changed" failure on startup** — If two processes bootstrapped the same brand-new agent connection at the same moment (e.g. starting several instances together for the first time), a rare race could cause one of them to register a fingerprint that didn't match what was actually saved. On the next restart this could look like the connection had changed, which is a hard failure for connections with `ProtectConnectionChanges` enabled. The fingerprint registration is now atomic, so this can no longer happen.

- **Cached reads could serve stale data far longer than expected after a change** — Agent connection saves/deletes and host registration/stats/deletion notified other processes' caches to refresh *after* writing the change. If anything went wrong between the write and that notification (including the process simply stopping), other processes kept serving pre-change data until their cache entry's normal expiry, rather than picking up the change promptly. The notification now always happens first, so a change is never silently missed.

---

## JobMaster 0.0.9-alpha / JobMaster.Dashboard 0.0.2-alpha

### Fixed

- **Dashboard: worker mode display** (`JobMaster.Dashboard`): the Workers Online KPI on the dashboard overview incorrectly swapped the `Execution` and `Full` worker counts. Workers running in `Full` mode were reported as `Execution` and vice versa.

- **Dashboard: OpenAPI auto-discovery** (`JobMaster.Api`, `JobMaster.Dashboard`): `FromOpenApiJson()` failed to auto-discover clusters and auth providers on first load. Two root causes: the `x-jobmaster-doc` extension value used to identify the JobMaster OpenAPI document was written incorrectly, and the `x-jobmaster-clusters` extension was written to the wrong location in the document. Both are now fixed — clusters and auth schemes are discovered correctly without any manual `ConfigCluster` or auth configuration.

- **Dashboard: config endpoint returning 500** (`JobMaster.Dashboard`): the `/jobmaster-config.json` endpoint returned a 500 error when the OpenAPI spec was not yet reachable at startup. The endpoint now returns 404 when the spec responds with 404, and includes a startup retry so transient timing issues are handled automatically.

---

## JobMaster.Dashboard 0.0.1-alpha

Initial release of the `JobMaster.Dashboard` package. The dashboard is versioned independently from the core JobMaster packages — it has no dependency on `JobMaster` or its agents and can be deployed alongside any JobMaster API instance.

### Added

- **`StartJobMasterDashboard()`**: single call that registers middleware and maps all dashboard endpoints. `UseJobMasterDashboard` and `MapJobMasterDashboard` are now internal — `StartJobMasterDashboard` is the only public entry point.

- **Dashboard overview KPIs**: Upcoming Execution breakdown (On Master, In Bucket, Onboarded, Queued, Processing), Failed Jobs count with configurable time window, Workers Online, Hosts, and Buckets cards.

- **Failed Jobs time window**: the Failed Jobs KPI respects the configured `lastHours` setting, passing `ScheduledFrom` to the API count endpoint so only jobs scheduled within the window are counted.

- **Status tooltips**: hovering any status row in the Upcoming Execution breakdown shows a description of what that status means in the job lifecycle.

- **Recently Executed Jobs table**: shows the most recent Succeeded and Failed jobs in a single merged query, sorted by finalized time.

- **Date display localisation**: all dates are displayed in the browser's local timezone using the browser's locale. UTC strings without a `Z` suffix are normalised before parsing so the timezone offset is always applied correctly.

- **Per-cluster theme assignment**: themes can be pinned to a specific cluster ID so users get an immediate visual cue about which environment they are in (e.g. blue for production, amber for QA).

- **`IncludeClusterIdsInOpenApi()`** on `JobMaster.Api`: embeds all registered cluster IDs in the OpenAPI document under `x-jobmaster-clusters`, enabling the dashboard's `FromOpenApiJson()` auto-discovery to resolve clusters without manual configuration.

- **`jobmaster-config.example.json`**: reference template for the dashboard's static config file, used when running the frontend standalone outside the C# host.

### Documentation

- `ApiConfiguration.md`: full guide covering cluster connection, base configuration, all authentication providers (API Key, User & Password, JWT Bearer), advanced customisation, and the isolated Swagger UI with cluster discovery.
- `DashboardConfiguration.md`: full guide covering registration, base path, cluster configuration, all auth provider types, auth retention, themes (including per-cluster assignment), OpenAPI auto-config, and a complete wiring example.

---

## JobMaster.Api 0.0.8-alpha

### Changed

- **Swagger document name**: the internal OpenAPI document is now registered under `"jobmaster"` instead of `"v1"`, ensuring it never conflicts with a host application that already uses `"v1"` as its own document name.

- **Swagger endpoint isolation**: endpoint filtering is now based solely on the `DocName` tag — the previous `x-jobmaster-doc` extension double-check on the inclusion predicate has been removed, simplifying the logic without changing behaviour.

- **Host docs in JobMaster UI**: the JobMaster Swagger UI now also surfaces any host application Swagger documents alongside the JobMaster document, so the isolated UI is a complete view of the application.

- **Guard for empty host Swagger configuration**: a fallback `"v1"` document is registered when the host application has no Swagger docs, preventing Swashbuckle from throwing on startup.

- **Default API key header corrected**: the Swagger security scheme default for API key auth was `"api-key"`, now `"X-Api-Key"` to match the actual header name used at runtime.

### Added

- **`IncludeClusterIdsInOpenApi()`** on `JobMasterApiOptions`: embeds all registered cluster IDs in the OpenAPI document info under the `x-jobmaster-clusters` extension key. Used by the dashboard's `FromOpenApiJson()` to auto-discover clusters without manual configuration.

---

## 0.0.7-alpha

### Added

- **`Onboarded` job status** (`= 10`): a new intermediate status between `InBucket` and
  `Queued`. Jobs in this state have been accepted by a bucket and are being prepared for
  execution. Visible in job queries, API responses, and status filters.

- **`UseNatsJetStream` multi-server overloads**: configure a NATS cluster connection by
  passing multiple servers as a `string[]` of connection strings or a
  `(url, userName, password)[]` tuple array. URL normalisation (`nats://` prefix,
  credential embedding) is handled automatically.
  ```csharp
  .UseNatsJetStream(new[] { "nats-1:4222", "nats-2:4222" }, userName, password)
  ```

- **`notifyAgent` parameter on `Schedule`/`ScheduleAsync`**: optional `bool notifyAgent = true`
  flag suppresses the agent wake-up notification when scheduling in bulk, reducing
  unnecessary traffic.

- **`JobMasterDefaults` static class**: a single reference point for all framework
  defaults — `DefaultJobTimeout` (1 min), `TransientThreshold` (10 min), `MaxRetryCount`
  (3), `MaxMessageByteSize` (128 KB), `DataRetentionTtl` (30 days), and worker defaults
  (`TransferBatchSize`, `BucketBufferSize`, `BucketBufferLeadTime`, etc.). Useful when
  you want to set a config value relative to the framework default.

- **`ApiJobExecution` enriched fields**: job execution records returned by the API now
  include `AgentConnectionName`, `HostId`, and `HostDisplayName`.

- **`ApiJobQueryCriteria` new filters**: the jobs query endpoint now accepts `HostId`,
  `BucketId`, `WorkerLane`, `AgentConnectionId`, and `WorkerId` filter parameters.

- **`ApiRecurringScheduleModel.IsStaticIdle`**: new boolean field in the recurring
  schedule API response — `true` when the schedule is static (startup-defined) but not
  currently active.

- **Swagger OpenAPI Auto-Documentation**: fully annotated all HTTP API request, response, and authentication DTOs with XML documentation comments, enabling auto-generation of clean OpenAPI (Swagger) specifications inside integration portals.

- **NATS busy-retry backoff**: when a bucket's onboarding buffer is full, the runner
  automatically retries with increasing delays (30 s → 75 s → 3 min). After three retries
  the job is returned to the master. No configuration required.

### Changes

- **Worker auto-name format changed**: auto-generated worker names are now
  `{hostname}-{timestampId}` (e.g. `myserver-3c1a8b2`). Explicitly named workers follow
  `{workerName}-{timestampId}` (e.g. `payroll-01-3c1a8b2`). The worker ID for explicit
  names is now derived from `workerName` instead of `hostId`, making it stable across
  restarts. See [WorkersConfiguration](docs/WorkersConfiguration.md) for details.

- **GUID v7 for all entity IDs** ⚠️ *breaking schema change*: jobs, recurring schedules,
  job executions, and distributed lock records now use time-ordered GUID v7 instead of
  random v4 GUIDs. This improves insert performance on large tables but requires a fresh
  database — existing v4 IDs are not migrated.

- **Dedicated `job_execution` and `log` tables** ⚠️ *breaking schema change*: job execution history and system logs are now stored in dedicated, highly indexed database tables (`job_execution` and `log` respectively) rather than generic record tables. This dramatically reduces table contention, but requires a fresh database setup or manual migrations.

- **Deadline runner is now a safety net only**: the deadline runner no longer races with
  the normal drain path. It only reclaims jobs when a bucket is lost or the drain fails.
  This reduces unnecessary job re-routing under normal load.

- **`ApiLogItem` and `ApiLogItemQueryCriteria`**: see **Renamed** below — the field names
  changed in a breaking way.

### Fixed

- **Lost jobs under high throughput (NATS)**: a bug caused the NATS consumer to silently
  stop receiving messages when the server closed an idle subscription (heartbeat expiry,
  server restart). Jobs already delivered but not yet acked would wait the full `AckWait`
  window before redelivery. The consumer now automatically restarts, and an idle heartbeat
  (5 s) keeps the subscription alive between messages.

- **Postpone duration too short**: when a bucket temporarily couldn't accept a job, the
  re-dispatch delay was missing a scaling factor (`PostponeFactor`), causing jobs to be
  retried sooner than intended and adding unnecessary load.

- **.NET 6 / .NET 7 compatibility**: the NATS connector's `IAsyncDisposable` code path
  was guarded by `#if NET8_0_OR_GREATER` — it now correctly applies from .NET 6 onward.

### Renamed ⚠️ breaking changes

- **`footprint` → `fingerprint`** (all layers): the agent connection fingerprint
  interface, method names, NATS KV key suffix (`agent_footprints` → `agent_fingerprints`),
  SQL table (`agent_conn_footprint` → `agent_conn_fingerprint`), and SQL column
  (`footprint` → `fingerprint`) are all renamed. Update any custom SQL queries or
  direct interface implementations.

- **`JobMasterLogSubjectType` → `JobMasterLogCategory`**: the enum is replaced and the
  API log model fields are renamed — `SubjectType` → `Category`, `SubjectId` →
  `ReferenceId`. Update any code that reads or filters log items via the API.

---

## 0.0.6-alpha

### Added
- **TriggerSourceTypes filter on JobQueryCriteria**

- **Host and Agent Connection Tracking**: Enhanced visibility and monitoring
  of infrastructure components
  - **Agent Connection Fingerprint**: Capture and persist agent connection
    metadata to detect configuration changes and prevent unexpected behavior
  - **Host Information**: Track which physical/virtual hosts are running
    workers, enabling better resource allocation and troubleshooting
  - **System Metrics Collection**: Abstract infrastructure (`IHostStatsProvider`)
    for collecting host statistics (CPU, RAM, disk I/O, network, etc.) to
    support future monitoring and auto-scaling capabilities
  - **New API endpoints**: `/hosts` and `/agent-connections` exposed via the
    REST API with pagination and filtering support

- **Generic Record Tables Reorganization**: Split generic record storage into
  specialized table families for better performance and maintainability
  - `generic_record_entry` / `generic_record_entry_value` — cluster config
    and general records
  - `generic_record_entry_topology` / `..._topology` — topology entities
    (Buckets, Workers, Hosts, Agent Connections)
  - `generic_record_entry_runtime` / `..._runtime` — runtime/heartbeat data
  - `generic_record_entry_log` / `..._log` — system logs
  - `generic_record_entry_job_metadata` / `..._job_metadata` — job metadata
  - `generic_record_entry_recurring_schedule_metadata` / `..._recurring_schedule_metadata`
    — recurring schedule metadata

- **Split BatchSize into TransferBatchSize, BucketBufferSize, and BucketBufferLeadTime**
  - **`TransferBatchSize`** — number of jobs pulled per DB round-trip. Default: `1000` (standalone: `250`).
  - **`BucketBufferSize`** — maximum jobs held in memory per bucket. Default: `250`.
  - **`BucketBufferLeadTime`** — how far ahead jobs are pre-loaded. Must be between `250ms` and `30s`. Default: `30s`.
  - See [WorkersConfiguration](docs/WorkersConfiguration.md) for sizing guidance.

- **Pagination and sorting for all API endpoints**

### Changes

- **Job Property Renames** *(migration required)*
  - `RecurringScheduleId` → `SourceId`
  - `ScheduledAt` → `NextPlanExecutionAt`
  - `OriginalScheduledAt` → `ScheduledAt`
  - `SucceedExecutedAt` → `FinalizedAt` (now also set on failure and cancellation)
  - `ProcessingStartedAt` → `ProcessStartedAt`
  - `PartitionLockId (int)` → `PartitionLockId (guid)`
  - **Migration Scripts** in [`migrations/0.0.6-alpha/`](migrations/0.0.6-alpha/)
    - ⚠️ **Alpha Notice**: Not fully tested. Recommended approach: let
      JobMaster create a fresh database. Only use migration scripts if you
      cannot afford to lose existing data, and always test in a lower
      environment first.
    - [PostgreSQL](migrations/0.0.6-alpha/job-properties-rename-migration-postgres.sql)
    - [SQL Server](migrations/0.0.6-alpha/job-properties-rename-migration-sqlserver.sql)
    - [MySQL](migrations/0.0.6-alpha/job-properties-rename-migration-mysql.sql)

- **Generic Record Table Migration Scripts** in [`migrations/0.0.6-alpha/`](migrations/0.0.6-alpha/)
  - ⚠️ Same alpha notice as above applies.
  - [PostgreSQL](migrations/0.0.6-alpha/generic-tables-family-migration-postgres.sql)
  - [SQL Server](migrations/0.0.6-alpha/generic-tables-family-migration-sqlserver.sql)
  - [MySQL](migrations/0.0.6-alpha/generic-tables-family-migration-mysql.sql)

### Fixes
- Fix pagination bug for SQL providers
- Create fallback bucket when no buckets were configured to the jobs
- Implement better fail policy for the Runners
- Improve dequeue performance with SQL provider-specific implementations
  (Postgres, SQL Server, MySQL each have optimized paths)
- Improve upsert performance on generic record repository with
  provider-specific implementations

## 0.0.5-alpha
### Added
- **Core API**: Implementation to consult all system entities (Jobs, Buckets, Workers, Clusters, etc.).
- **Standalone Mode**: Quick-start configuration for users who do not require multiple agents or external brokers.
- **Performance Optimization**: Improved job fetching logic with AcquireAndFetchAsync to reduce database round-trips.
- **ReadIsolationLevel**: Add ReadIsolationLevel some we can have dirty reads. e.g API, Logs and counts.
- **Improve the config selector internal code**: get ride of the internal advance selector.

## 0.0.4-alpha
### Added
- Rename AgentWorkerMode.Standalone to AgentWorkerMode.Full
- Fix project, classes, namespace typo (NatJetStream -> NatsJetStream)
- Consolidate/Rename ScheduleType and ScheduledSourceType to TriggerSourceType
  - DB change required if using JobMaster v0.0.3
    - Postgres:
      ```sql 
      ALTER TABLE your_table RENAME COLUMN schedule_type TO trigger_source_type;
      ```
    - SQL Server: 
      ```sql 
      EXEC sp_rename 'dbo.your_table.schedule_type', 'trigger_source_type', 'COLUMN';
      ```
    - MySQL: 
      ```sql 
      ALTER TABLE your_table RENAME COLUMN schedule_type TO trigger_source_type;
      ```
- Make SDK.Abstractions internal and expose what is needed
- Make Utils and Utils.Extensions internal and move to JobMaster.Sdk namespace
