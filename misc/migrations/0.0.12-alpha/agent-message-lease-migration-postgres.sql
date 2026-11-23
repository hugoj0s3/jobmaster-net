-- JobMaster v0.0.12-alpha Migration Script
-- Database: PostgreSQL (every agent connection database, and the master database)
-- Purpose: Add the lease columns to the agent message table. Saving pending jobs and recurring schedules now leases
--          their messages and deletes them only once they are on master, so a stopped or killed worker no longer
--          loses the batch it was saving. lease_count counts how many times a message was leased.
-- Note: New installs get the columns automatically. Run this once on databases created by an earlier version,
--       before starting 0.0.12 workers. leased_until is nullable and lease_count defaults to 0, so existing rows
--       stay valid.
--
-- IMPORTANT: Replace [your-prefix] with your actual table prefix (default is 'jm_')
--            Use Find & Replace: [your-prefix] -> jm_ (or your custom prefix)

ALTER TABLE [your-prefix]message_dispatcher ADD COLUMN IF NOT EXISTS leased_until TIMESTAMP WITHOUT TIME ZONE NULL;
ALTER TABLE [your-prefix]message_dispatcher ADD COLUMN IF NOT EXISTS lease_count INTEGER NOT NULL DEFAULT 0;
