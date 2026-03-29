# JobMaster – Reminders & Improvement Backlog

## Scheduler
- **Locking strategy**: Re‑evaluate the locker usage (we probably no longer need the “saving” locks). Ideal design is per-resource locking; keep the current action-level approach only if the refactor is too costly right now.
- **Missing handler fallout (`RecurringSchedulePlanner.ScheduleNextJobsAsync`)**: Current guard just logs and skips when a handler is gone; add logic to automatically terminate the recurring schedule after X consecutive errors or a time threshold so logs don’t spam forever.

## Runners
- **Graceful stop**: Review and refine the shutdown sequence—the current implementation is unreliable.
- **Immediate stop**: Improve the hard-stop path as well; it still behaves awkwardly.

## Ideas & Experiments

### Cluster-aware bulk persistence
Goal: let `JobMasterSchedulerClusterAware` batch job/recurring schedule writes.

Possible steps:
1. Buffer schedules in memory and force a save after X minutes.
2. Flush buffered jobs in bulk to the generic repository.
3. Partition buffers (e.g., 100 jobs per record) so each write is compact.
4. Flush either on a timer or when the buffer hits the item threshold.
5. Add a runner that pulls partitions by group id and moves them to the master DB.
6. If stable, explore reusing the mechanism for `SaveOperation` (watch for conflicts with the scheduler component).

### JobDefinition-based scheduling
Goal: support advanced scenarios where publishers and consumers are fully separated.

Approach:
1. Introduce a `JobDefinition` class that encapsulates timeouts and configuration.
2. Allow handlers such as `JobHandlerA : IJobHandler<DefinitionJobA>` so consumers can bind to definitions directly.
3. Keep the current “direct handler” option for simple scenarios, and consider releasing the definitional model as a v2 feature.


