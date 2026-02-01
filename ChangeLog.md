# ChangeLog
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