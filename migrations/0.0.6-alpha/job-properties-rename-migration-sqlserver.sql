-- JobMaster v0.0.6-alpha Migration Script
-- Database: SQL Server
-- Purpose: Migrate job table property renames
-- Note: Since this is an alpha version, you may choose to recreate your database from scratch
--       instead of running this migration script. If really need to run it to not lose existing data
--       test in lower environment first.
--
-- IMPORTANT: Replace [your-prefix] with your actual table prefix (default is 'jm_')
--            Use Find & Replace: [your-prefix] -> jm_ (or your custom prefix)
--
-- Property renames:
--   - RecurringScheduleId -> SourceId
--   - ScheduledAt -> NextPlanExecutionAt
--   - OriginalScheduledAt -> ScheduledAt (if exists)

-- Step 1: Rename RecurringScheduleId to SourceId
EXEC sp_rename 'dbo.[your-prefix]job.recurring_schedule_id', 'source_id', 'COLUMN';

-- Step 2: Add new NextPlanExecutionAt column
ALTER TABLE dbo.[your-prefix]job ADD next_plan_execution_at DATETIME2 NOT NULL DEFAULT GETUTCDATE();

-- Step 3: Copy ScheduledAt to NextPlanExecutionAt
UPDATE dbo.[your-prefix]job SET next_plan_execution_at = scheduled_at;

-- Step 4: Rename OriginalScheduledAt to ScheduledAt (if OriginalScheduledAt exists)
-- If you don't have original_scheduled_at column, skip this step
EXEC sp_rename 'dbo.[your-prefix]job.original_scheduled_at', 'scheduled_at_temp', 'COLUMN';
EXEC sp_rename 'dbo.[your-prefix]job.scheduled_at', 'next_plan_execution_at_temp', 'COLUMN';
EXEC sp_rename 'dbo.[your-prefix]job.scheduled_at_temp', 'scheduled_at', 'COLUMN';
ALTER TABLE dbo.[your-prefix]job DROP COLUMN next_plan_execution_at_temp;
