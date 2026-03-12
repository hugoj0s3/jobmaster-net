# ChangeLog
## 0.0.6-alpha
### Added
- TriggerSourceTypes on JobQueryCriteria
### Changes
- Job Properties Renames
  - Rename from RecurringScheduleId to SourceId { get; set; }
  - Rename from ScheduledAt to NextPlanExecutionAt
  - Rename from OriginalScheduledAt to ScheduledAt
  - **Migration Scripts**: See migration scripts in [`migrations/0.0.6-alpha/`](migrations/0.0.6-alpha/)
    - ⚠️ **Alpha Version Notice**: These migration scripts are not fully tested. **Recommended approach**: Let JobMaster create a fresh database with the new schema. **Only use migration scripts** if you cannot afford to lose existing data, and always test thoroughly in a lower environment first.
    - [PostgreSQL migration script](migrations/0.0.6-alpha/job-properties-rename-migration-postgres.sql)
    - [SQL Server migration script](migrations/0.0.6-alpha/job-properties-rename-migration-sqlserver.sql)
    - [MySQL migration script](migrations/0.0.6-alpha/job-properties-rename-migration-mysql.sql)
- **Generic Record Tables Reorganization**: Split generic record storage into specialized table families for better performance and maintainability
  - **Table Families**:
    - `generic_record_entry` / `generic_record_entry_value` (default) - For cluster configuration and other general records
    - `generic_record_entry_topology` / `generic_record_entry_value_topology` - For topology entities (Buckets, Workers, Hosts, Agent Connections)
    - `generic_record_entry_runtime` / `generic_record_entry_value_runtime` - For runtime/heartbeat data (Sentinel, Worker/Agent/Host heartbeats)
    - `generic_record_entry_log` / `generic_record_entry_value_log` - For system logs
    - `generic_record_entry_job_metadata` / `generic_record_entry_value_job_metadata` - For job metadata
    - `generic_record_entry_recurring_schedule_metadata` / `generic_record_entry_value_recurring_schedule_metadata` - For recurring schedule metadata
  - **Benefits**: Improved query performance, easier maintenance, and better data isolation by separating high-volume transient data (logs, heartbeats) from stable configuration data
  - **Migration Scripts**: See migration scripts in [`migrations/0.0.6-alpha/`](migrations/0.0.6-alpha/)
    - ⚠️ **Alpha Version Notice**: These migration scripts are not fully tested. **Recommended approach**: Let JobMaster create a fresh database with the new schema. **Only use migration scripts** if you cannot afford to lose existing data, and always test thoroughly in a lower environment first.
    - [PostgreSQL migration script](migrations/0.0.6-alpha/generic-tables-family-migration-postgres.sql)
    - [SQL Server migration script](migrations/0.0.6-alpha/generic-tables-family-migration-sqlserver.sql)
    - [MySQL migration script](migrations/0.0.6-alpha/generic-tables-family-migration-mysql.sql)
- **Host and Agent Connection Tracking**: Enhanced visibility and monitoring of infrastructure components
  - **Agent Connection Footprint**: Capture and persist agent connection metadata to detect configuration changes and prevent unexpected behavior
  - **Host Information**: Track which physical/virtual hosts are running workers, enabling better resource allocation and troubleshooting
  - **System Metrics Collection**: Abstract infrastructure for collecting host statistics (CPU usage, RAM, disk I/O, network, etc.) to support future monitoring and auto-scaling capabilities
- Separate TransferBatchSize, BucketBufferSize and introduce BucketBufferLeadTime
- Fix Pagination bug for SQL
- Fix miss rollbacks, remove keep alive connection from dequeuemessageasync, provide retry mecanism for dequeuemessage async.
- Introduce pagination and sort for all api endpoints. (keep the standard for dashboard)
- Improve the dequeue to improve the performance make the code very specific for each SQL dbs
- Improve the upinsert of the generic repo (code very specific for each sql dbs.)

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