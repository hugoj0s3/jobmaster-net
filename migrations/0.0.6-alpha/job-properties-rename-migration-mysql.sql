-- JobMaster v0.0.6-alpha Migration Script
-- Database: MySQL
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
--   - SucceedExecutedAt -> FinalizedAt
--   - ProcessingStartedAt -> ProcessStartedAt

-- Step 1: Rename RecurringScheduleId to SourceId
ALTER TABLE [your-prefix]job RENAME COLUMN recurring_schedule_id TO source_id;

-- Step 2: Add new NextPlanExecutionAt column
ALTER TABLE [your-prefix]job ADD COLUMN next_plan_execution_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- Step 3: Copy ScheduledAt to NextPlanExecutionAt
UPDATE [your-prefix]job SET next_plan_execution_at = scheduled_at;

-- Step 4: Rename OriginalScheduledAt to ScheduledAt (if OriginalScheduledAt exists)
-- If you don't have original_scheduled_at column, skip this step
ALTER TABLE [your-prefix]job CHANGE COLUMN original_scheduled_at scheduled_at_temp DATETIME NOT NULL;
ALTER TABLE [your-prefix]job CHANGE COLUMN scheduled_at next_plan_execution_at_temp DATETIME NOT NULL;
ALTER TABLE [your-prefix]job CHANGE COLUMN scheduled_at_temp scheduled_at DATETIME NOT NULL;
ALTER TABLE [your-prefix]job DROP COLUMN next_plan_execution_at_temp;

-- Step 5: Rename SucceedExecutedAt to FinalizedAt
ALTER TABLE [your-prefix]job RENAME COLUMN succeed_executed_at TO finalized_at;

-- Step 6: Rename ProcessingStartedAt to ProcessStartedAt
ALTER TABLE [your-prefix]job RENAME COLUMN processing_started_at TO process_started_at;
