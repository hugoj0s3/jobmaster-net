-- JobMaster v0.0.6-alpha Migration Script
-- Database: SQL Server
-- Purpose: Migrate generic_record_entry tables to family-based structure
-- Note: Since this is an alpha version, you may choose to recreate your database from scratch
--       instead of running this migration script. If really need to run it to not lose existing data
--       test in lower environment first.
--
-- IMPORTANT: Replace [your-prefix] with your actual table prefix (default is 'jm_')
--            Use Find & Replace: [your-prefix] -> jm_ (or your custom prefix)
--
-- IMPORTANT: For SQL Server, the family tables must be created beforehand with proper
--            schema definitions (primary keys, indexes, constraints).
--            This script only migrates the data.
--
-- You can create the tables using your DDL scripts or ORM migrations before running this.

-- Verify topology family tables exist (will fail if not created)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '[your-prefix]generic_record_entry_topology')
    THROW 50001, 'Table [your-prefix]generic_record_entry_topology must be created before running migration', 1;
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '[your-prefix]generic_record_entry_value_topology')
    THROW 50001, 'Table [your-prefix]generic_record_entry_value_topology must be created before running migration', 1;

-- Migrate topology data
INSERT INTO [your-prefix]generic_record_entry_topology 
SELECT * FROM [your-prefix]generic_record_entry 
WHERE group_id IN ('Bucket', 'AgentWorker', 'Host', 'AgentConnection');

INSERT INTO [your-prefix]generic_record_entry_value_topology 
SELECT v.* FROM [your-prefix]generic_record_entry_value v
INNER JOIN [your-prefix]generic_record_entry e ON v.record_unique_id = e.record_unique_id
WHERE e.group_id IN ('Bucket', 'AgentWorker', 'Host', 'AgentConnection');

-- Verify runtime family tables exist (no data migration - runtime data is transient)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '[your-prefix]generic_record_entry_runtime')
    THROW 50001, 'Table [your-prefix]generic_record_entry_runtime must be created before running migration', 1;
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '[your-prefix]generic_record_entry_value_runtime')
    THROW 50001, 'Table [your-prefix]generic_record_entry_value_runtime must be created before running migration', 1;

-- Verify log family tables exist
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '[your-prefix]generic_record_entry_log')
    THROW 50001, 'Table [your-prefix]generic_record_entry_log must be created before running migration', 1;
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '[your-prefix]generic_record_entry_value_log')
    THROW 50001, 'Table [your-prefix]generic_record_entry_value_log must be created before running migration', 1;

-- Migrate log data
INSERT INTO [your-prefix]generic_record_entry_log 
SELECT * FROM [your-prefix]generic_record_entry WHERE group_id = 'Log';

INSERT INTO [your-prefix]generic_record_entry_value_log 
SELECT v.* FROM [your-prefix]generic_record_entry_value v
INNER JOIN [your-prefix]generic_record_entry e ON v.record_unique_id = e.record_unique_id
WHERE e.group_id = 'Log';

-- Verify job metadata family tables exist
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '[your-prefix]generic_record_entry_job_metadata')
    THROW 50001, 'Table [your-prefix]generic_record_entry_job_metadata must be created before running migration', 1;
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '[your-prefix]generic_record_entry_value_job_metadata')
    THROW 50001, 'Table [your-prefix]generic_record_entry_value_job_metadata must be created before running migration', 1;

-- Migrate job metadata
INSERT INTO [your-prefix]generic_record_entry_job_metadata 
SELECT * FROM [your-prefix]generic_record_entry WHERE group_id = 'JobMetadata';

INSERT INTO [your-prefix]generic_record_entry_value_job_metadata 
SELECT v.* FROM [your-prefix]generic_record_entry_value v
INNER JOIN [your-prefix]generic_record_entry e ON v.record_unique_id = e.record_unique_id
WHERE e.group_id = 'JobMetadata';

-- Verify recurring schedule metadata family tables exist
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '[your-prefix]generic_record_entry_recurring_schedule_metadata')
    THROW 50001, 'Table [your-prefix]generic_record_entry_recurring_schedule_metadata must be created before running migration', 1;
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '[your-prefix]generic_record_entry_value_recurring_schedule_metadata')
    THROW 50001, 'Table [your-prefix]generic_record_entry_value_recurring_schedule_metadata must be created before running migration', 1;

-- Migrate recurring schedule metadata
INSERT INTO [your-prefix]generic_record_entry_recurring_schedule_metadata 
SELECT * FROM [your-prefix]generic_record_entry WHERE group_id = 'RecurringScheduleMetadata';

INSERT INTO [your-prefix]generic_record_entry_value_recurring_schedule_metadata 
SELECT v.* FROM [your-prefix]generic_record_entry_value v
INNER JOIN [your-prefix]generic_record_entry e ON v.record_unique_id = e.record_unique_id
WHERE e.group_id = 'RecurringScheduleMetadata';
