# JobMaster – Reminders & Improvement Backlog

## Scheduler
- **Locking strategy**: Re‑evaluate the locker usage (we probably no longer need the “saving” locks). Ideal design is per-resource locking; keep the current action-level approach only if the refactor is too costly right now.
- **Missing handler fallout (`RecurringSchedulePlanner.ScheduleNextJobsAsync`)**: Current guard just logs and skips when a handler is gone; add logic to automatically terminate the recurring schedule after X consecutive errors or a time threshold so logs don’t spam forever.

## Runners
- **Graceful stop**: Review and refine the shutdown sequence—the current implementation is unreliable.
- **Immediate stop**: Improve the hard-stop path as well; it still behaves awkwardly.
- **ScanPlanner**: move LockerSlot selection into ComputeScanPlanHalfWindow result reuse across the runners.

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


