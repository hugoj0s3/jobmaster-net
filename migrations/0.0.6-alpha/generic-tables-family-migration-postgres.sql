-- JobMaster v0.0.6-alpha Migration Script
-- Database: PostgreSQL
-- Purpose: Migrate generic_record_entry tables to family-based structure
-- Note: Since this is an alpha version, you may choose to recreate your database from scratch
--       instead of running this migration script. If really need to run it to not lose existing data
--       test in lower environment first.
--
-- IMPORTANT: Replace [your-prefix] with your actual table prefix (default is 'jm_')
--            Use Find & Replace: [your-prefix] -> jm_ (or your custom prefix)
--
-- LIKE ... INCLUDING ALL automatically copies:
--       - Primary keys
--       - Indexes
--       - Constraints (foreign keys, unique, check)
--       - Defaults

-- Create topology family tables (idempotent)
CREATE TABLE IF NOT EXISTS [your-prefix]generic_record_entry_topology (LIKE [your-prefix]generic_record_entry INCLUDING ALL);
CREATE TABLE IF NOT EXISTS [your-prefix]generic_record_entry_value_topology (LIKE [your-prefix]generic_record_entry_value INCLUDING ALL);

-- Migrate topology data (Bucket, AgentWorker, Host, AgentConnection)
INSERT INTO [your-prefix]generic_record_entry_topology 
SELECT * FROM [your-prefix]generic_record_entry 
WHERE group_id IN ('Bucket', 'AgentWorker', 'Host', 'AgentConnection');

INSERT INTO [your-prefix]generic_record_entry_value_topology 
SELECT v.* FROM [your-prefix]generic_record_entry_value v
INNER JOIN [your-prefix]generic_record_entry e ON v.record_unique_id = e.record_unique_id
WHERE e.group_id IN ('Bucket', 'AgentWorker', 'Host', 'AgentConnection');

-- Create runtime family tables (no data migration - runtime data is transient)
CREATE TABLE IF NOT EXISTS [your-prefix]generic_record_entry_runtime (LIKE [your-prefix]generic_record_entry INCLUDING ALL);
CREATE TABLE IF NOT EXISTS [your-prefix]generic_record_entry_value_runtime (LIKE [your-prefix]generic_record_entry_value INCLUDING ALL);

-- Create log family tables
CREATE TABLE IF NOT EXISTS [your-prefix]generic_record_entry_log (LIKE [your-prefix]generic_record_entry INCLUDING ALL);
CREATE TABLE IF NOT EXISTS [your-prefix]generic_record_entry_value_log (LIKE [your-prefix]generic_record_entry_value INCLUDING ALL);

INSERT INTO [your-prefix]generic_record_entry_log 
SELECT * FROM [your-prefix]generic_record_entry WHERE group_id = 'Log';

INSERT INTO [your-prefix]generic_record_entry_value_log 
SELECT v.* FROM [your-prefix]generic_record_entry_value v
INNER JOIN [your-prefix]generic_record_entry e ON v.record_unique_id = e.record_unique_id
WHERE e.group_id = 'Log';

-- Create job metadata family tables
CREATE TABLE IF NOT EXISTS [your-prefix]generic_record_entry_job_metadata (LIKE [your-prefix]generic_record_entry INCLUDING ALL);
CREATE TABLE IF NOT EXISTS [your-prefix]generic_record_entry_value_job_metadata (LIKE [your-prefix]generic_record_entry_value INCLUDING ALL);

INSERT INTO [your-prefix]generic_record_entry_job_metadata 
SELECT * FROM [your-prefix]generic_record_entry WHERE group_id = 'JobMetadata';

INSERT INTO [your-prefix]generic_record_entry_value_job_metadata 
SELECT v.* FROM [your-prefix]generic_record_entry_value v
INNER JOIN [your-prefix]generic_record_entry e ON v.record_unique_id = e.record_unique_id
WHERE e.group_id = 'JobMetadata';

-- Create recurring schedule metadata family tables
CREATE TABLE IF NOT EXISTS [your-prefix]generic_record_entry_recurring_schedule_metadata (LIKE [your-prefix]generic_record_entry INCLUDING ALL);
CREATE TABLE IF NOT EXISTS [your-prefix]generic_record_entry_value_recurring_schedule_metadata (LIKE [your-prefix]generic_record_entry_value INCLUDING ALL);

INSERT INTO [your-prefix]generic_record_entry_recurring_schedule_metadata 
SELECT * FROM [your-prefix]generic_record_entry WHERE group_id = 'RecurringScheduleMetadata';

INSERT INTO [your-prefix]generic_record_entry_value_recurring_schedule_metadata 
SELECT v.* FROM [your-prefix]generic_record_entry_value v
INNER JOIN [your-prefix]generic_record_entry e ON v.record_unique_id = e.record_unique_id
WHERE e.group_id = 'RecurringScheduleMetadata';

-- Clean up old data (OPTIONAL - only after verifying migration)
-- Note: Runtime data (heartbeats, sentinel) is not migrated as it's transient
-- DELETE FROM [your-prefix]generic_record_entry_value WHERE record_unique_id IN (
--   SELECT record_unique_id FROM [your-prefix]generic_record_entry 
--   WHERE group_id IN ('Bucket', 'AgentWorker', 'Host', 'AgentConnection',
--                      'Log', 'JobMetadata', 'RecurringScheduleMetadata')
-- );
-- DELETE FROM [your-prefix]generic_record_entry 
-- WHERE group_id IN ('Bucket', 'AgentWorker', 'Host', 'AgentConnection',
--                    'Log', 'JobMetadata', 'RecurringScheduleMetadata');
