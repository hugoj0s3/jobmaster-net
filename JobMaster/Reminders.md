# JobMaster – Reminders & Improvement

## Scheduler
- **Locking strategy**: Re‑evaluate the locker usage (we probably no longer need the “saving” locks). Ideal design is per-resource locking; keep the current action-level approach only if the refactor is too costly right now.
- **Missing handler fallout (`RecurringSchedulePlanner.ScheduleNextJobsAsync`)**: Current guard just logs and skips when a handler is gone; add logic to automatically terminate the recurring schedule after X consecutive errors or a time threshold so logs don’t spam forever.

## Repository / Persistence
- **`BulkUpdateAsync` — single SQL round-trip**: The current implementation issues one SQL statement per job. Replace with a single batched statement (e.g. a `VALUES` list joined to the target table) that atomically checks the row version, applies the update, and returns only the rows that were actually changed. This will likely require DB-specific SQL (e.g. PostgreSQL `UPDATE … FROM (VALUES …)` or SQL Server `MERGE`), so abstract behind the existing db-provider pattern.

## Runners
- **Graceful stop**: Review and refine the shutdown sequence—the current implementation is unreliable.
- **Immediate stop**: Improve the hard-stop path as well; it still behaves awkwardly.
- **ScanPlanner**: move LockerSlot selection into ComputeScanPlanHalfWindow result reuse across the runners.
- **Cache for bucket and worker reads**: Runners currently call `QueryAllNoCacheAsync` for buckets (and workers) on every tick, causing redundant DB round-trips. Evaluate switching to the in-memory cache (`QueryAllAsync` or a dedicated cache key) so hot reads are served from memory. Invalidate on write so correctness is preserved; profile the cache-miss rate before deciding on TTL vs. event-driven invalidation.

## Ideas & Experiments

### Cluster-aware bulk persistence

Jobs scheduled through `JobMasterSchedulerClusterAware` are currently written to the agent first, then asynchronously synced to the master DB. This is intentional for performance — especially with message broker agents — but creates a durability gap: if the agent goes down or gets corrupted before the sync completes, those jobs are lost.
the risk should be very rare or zero in healthy clusters.
The goal is to introduce a durable buffering layer that batches schedule writes and ensures they reach the master DB reliably, even under failure conditions.

**Proposed approach:**

1. **In-memory buffer** — accumulate scheduled jobs in memory inside `JobMasterSchedulerClusterAware` rather than dispatching each one immediately.
2. **Bulk flush** — write buffered jobs to the generic repository in a single batched operation, reducing round-trips and lock contention on the master DB.
3. **Partitioned records** — group jobs into fixed-size partitions (e.g., 100 jobs per record) so each write remains compact and independently recoverable.
4. **Dual flush triggers** — flush either when the buffer reaches the item threshold or when a configurable time window elapses, whichever comes first.
5. **Partition runner** — introduce a background runner that polls for unflushed partitions by group ID and promotes them to the master DB, decoupling write buffering from master DB availability.
6. **`SaveOperation` reuse** — once stable, evaluate whether the same mechanism can back `SaveOperation` writes. Requires careful analysis to avoid conflicts with the scheduler's own flush cycle.

> ⚠️ This feature introduces a write-ahead style buffer. The failure window shrinks to the flush interval rather than being eliminated entirely — operators should configure the flush interval based on their durability tolerance.

### JobDefinition-based scheduling
Goal: support advanced scenarios where publishers and consumers are fully separated.

Approach:
1. Introduce a `JobDefinition` class that encapsulates timeouts and configuration.
2. Allow handlers such as `JobHandlerA : IJobHandler<DefinitionJobA>` so consumers can bind to definitions directly.
3. Keep the current “direct handler” option for simple scenarios, and consider releasing the definitional model as a v2 feature.

## Serialization
- **Source Generator Context (`JsonSerializerContext`)**: Upgrade `InternalJobMasterSerializer` to use `System.Text.Json` Source Generators. By declaring the exact types being serialized (e.g., `JobRawModel`, `Dictionary<string, object?>`), we can completely eliminate runtime reflection for JSON parsing, achieving AOT-level speed with zero allocations.

## Messaging & Positioning
- **Standalone Mode is a Selling Point:** The documentation mentions that reverting from Distributed to Standalone is a one-way operation. We need to re-word this or handle it gracefully to prevent scaring off early adopters. Standalone is a huge benefit (start simple, scale to NATS later while keeping the code).
- **Fallback Bucket Starvation Penalty:** Add a limit or "never processed" flag for fallback buckets to prevent memory starvation on coordinator nodes if a user misconfigures lane priorities.
