# ChangeLog
## 0.0.7-alpha
### Added
- **`Onboarded` job status** (`= 10`, between `InBucket` and `Queued`): marks a job that
  has been accepted into a bucket's onboarding buffer and had its `ProcessDeadline` set to
  `UtcNow` as an instant recovery signal — if the bucket goes Lost, the deadline runner
  picks it up immediately without waiting for a natural expiry.

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

### Changes
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

## 0.0.6-alpha
### Added
- **TriggerSourceTypes filter on JobQueryCriteria**

- **Host and Agent Connection Tracking**: Enhanced visibility and monitoring
  of infrastructure components
  - **Agent Connection Footprint**: Capture and persist agent connection
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
  - **Benefits**: Better query performance, easier maintenance, and isolation
    of high-volume transient data (logs, heartbeats) from stable config data

- **Split BatchSize into TransferBatchSize, BucketBufferSize, and BucketBufferLeadTime**
  - `BatchSize` has been removed. Its responsibilities are now split across three dedicated settings:
  - **`TransferBatchSize`** — number of jobs pulled per DB round-trip when the Coordinator transfers jobs from the Master DB into Agent Buckets and during other bulk operations. Default: `1000` (standalone: `250`).
  - **`BucketBufferSize`** — maximum number of jobs held in memory per bucket while awaiting execution. When the buffer is full, excess deliveries are bounced back to the Master with a short delay. Default: `250`.
  - **`BucketBufferLeadTime`** — how far ahead in time the worker pre-loads jobs into the in-memory buffer. Must be between `250ms` and `30s`. Default: `30s`.
  - See [WorkersConfiguration](docs/WorkersConfiguration.md) for sizing guidance.

- **Pagination and sorting for all API endpoints**
- ** 

### Changes

- **Job Property Renames** *(migration required)*
  - `RecurringScheduleId` → `SourceId`
  - `ScheduledAt` → `NextPlanExecutionAt`
  - `OriginalScheduledAt` → `ScheduledAt`
  - `SucceedExecutedAt` → `FinalizedAt` (now also set on failure and cancellation)
  - `ProcessingStartedAt` → `ProcessStartedAt`
  -  `PartitionLockId (int)` -> `PartitionLockId (guid)`
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
- 

### Fixes
- Fix pagination bug for SQL providers
- Create fallback bucket when no buckets were configured to the jobs.
- Implement better fail policy for the Runners
- Reduce the concurrency of the AcquireAndFetchAsync method and introduce debug policies.
- Rename `DequeueMessageAsync` to `PullMessageAsync`
- Remove keep-alive connection from `DequeueMessageAsync` to prevent
  connection contention during message dequeue
- Add retry mechanism for `DequeueMessageAsync`
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