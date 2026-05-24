# ChangeLog

> **Audience: framework users.**
> Documents new features, breaking changes, and user-visible fixes for each release.
> For internal implementation details see [ChangeLog.internal.md](ChangeLog.internal.md).

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

- **Dedicated `job_execution` table** ⚠️ *breaking schema change*: job execution records
  are now stored in a dedicated `job_execution` table instead of the generic record tables.
  Requires a fresh database or a manual migration.

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
