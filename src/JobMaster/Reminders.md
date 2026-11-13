# JobMaster – Reminders & Improvement

## Scheduler
- **Locking strategy**: Re‑evaluate the locker usage (we probably no longer need the “saving” locks). Ideal design is per-resource locking; keep the current action-level approach only if the refactor is too costly right now.
- **Missing handler fallout (`RecurringSchedulePlanner.ScheduleNextJobsAsync`)**: Current guard just logs and skips when a handler is gone; add logic to automatically terminate the recurring schedule after X consecutive errors or a time threshold so logs don’t spam forever.

## Repository / Persistence
- **`BulkUpdateAsync` — single SQL round-trip**: The current implementation issues one SQL statement per job. Replace with a single batched statement (e.g. a `VALUES` list joined to the target table) that atomically checks the row version, applies the update, and returns only the rows that were actually changed. This will likely require DB-specific SQL (e.g. PostgreSQL `UPDATE … FROM (VALUES …)` or SQL Server `MERGE`), so abstract behind the existing db-provider pattern.
- **`DisableAutoProvisionSqlSchema` still global, not per-connection**: The `tablePrefix` SQL setting was moved off the generic `UseSqlTablePrefixForMaster`/`UseSqlTablePrefixForAgent` methods (removed in 0.0.12) onto each provider's own `UsePostgresForMaster(..., tablePrefix:)`/etc. parameter, because it's genuinely per-connection. `DisableAutoProvisionSqlSchema` was deliberately left out of that pass — `SqlJobMasterRuntimeSetup.ConfigAgentsAsync` reads the *cluster's own* flag even when deciding whether to provision *agent* tables, so today it applies uniformly to the master DB and every agent connection in that cluster, with no independent per-connection switch. Revisit if a real use case for per-connection granularity comes up (e.g. auto-provision the master DB but manage one particular agent DB's schema manually) — at that point it could follow the same per-provider-parameter + JSON-binder pattern `tablePrefix` now uses.

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

### JobDefinition-based scheduling — static recurring schedules still open
One-time and dynamic-recurring scheduling via `IJobMasterScheduler.Advanced` (`JobDefinitionConfig`/
`JobDefinitionConfigAttribute`/`IJobMasterSchedulerAdvanced`) shipped in 0.0.11-alpha — see ChangeLog.md.
Still open: `RecurringScheduleDefinitionCollection.Add<Th>()` (static recurring schedules) has the same
handler-type coupling and hasn't been given the same treatment yet.

## Public API / Namespaces
- **Clean up `JobMaster.Abstractions`**: It should hold only interfaces meant for external implementation (still the intent, e.g. storage providers), even though many of those implementations were later made internal. Over time concrete public types landed there too (`JobDefinitionConfig`, `JobMasterDefaults`, `NeverRecursExprCompiler`, `RecurrenceCompilerFactory`, the handler attributes in `Abstractions.Models.Attributes`, model enums, ...). Move them to namespaces that match their role, the way `JobMaster.Serialization` now holds the concrete serializers while `IJobMasterMessageJsonSerializer` stays in `Abstractions.Serialization`. Breaking namespace change for users: do it in one pass during alpha (ChangeLog entry, no `[Obsolete]`), not piecemeal.

## Tests
- **Exact-text `WithMessage` assertions**: A handful of tests assert on exact or substring exception message text (e.g. `JobMasterSchedulerClusterAwareTests.cs` asserts the full string `"Cluster mode is archived"`; several others assert `"*keyword*"` substrings). Prefer asserting exception type (and, where the specific culprit matters, a targeted property) instead — message text is easy to reword incidentally and breaks these tests for no functional reason. Left as-is for now (out of scope for the 0.0.10-alpha PR); worth a pass to check every `WithMessage`/`.Message.Should()` usage across the suite and loosen or remove where the wording isn't the actual thing under test.

## Serialization
- **Enum store format: `SetEnumValue<TEnum>(key, value, StoreFormat = Number)`**: Today `SetEnumValue` (MsgData) is persisted as a number, because `InternalJobMasterSerializer` uses default STJ options. `StoreFormat` (`Number` / `Name`) is needed, not just nice to have (Hugo): both formats carry a versioning risk, so the user picks the one that fits how their enum evolves. A number breaks when members are renumbered or reordered (implicit values); a name breaks when a member is renamed. Only the write side changes: `TryGetEnumValue` already accepts a name or a number and checks it's a defined member (`[Flags]`: all bits defined), so either risk surfaces as an exception, not a wrong value.
- **Hypothesis: put enum support back on Metadata**: It was removed in 0.0.11 (with `byte[]`) because it "round-trips ambiguously". That no longer holds for reads: the read is typed, so a stored number or name is rebuilt and checked, as MsgData does since 0.0.12. What's left to decide is how it looks where metadata is used:
  - **Dashboard / API**: a number shows as `1`, a name as `Green`.
  - **Metadata filters**: they match the stored value, so `Color = 1` or `Color = "Green"`. Mixing formats on one key means a filter only matches half the data, so the format should be one per key (or per cluster), not per call.
  - **Write side**: `SetEnumValue` should come back with `StoreFormat`, same reason as MsgData. Smallest first step if needed: only `TryGetEnumValue` / `GetEnumValue` on `IReadableMetadata`, with users writing through `SetStringValue` / `SetIntValue` (as the 0.0.11 ChangeLog already advises), which already lets them choose the format.
  - `byte[]` stays off Metadata: useless for filtering and display, and metadata should stay small.
- **Source Generator Context (`JsonSerializerContext`)**: Upgrade `InternalJobMasterSerializer` to use `System.Text.Json` Source Generators. By declaring the exact types being serialized (e.g., `JobRawModel`, `Dictionary<string, object?>`), we can completely eliminate runtime reflection for JSON parsing, achieving AOT-level speed with zero allocations.

## Benchmarks
- **Re-run the RavenDB+NATS benchmark with capped (not unlimited) worker CPU/memory**: Local testing (2026-09-07/08) found that uncapped worker CPU quietly made `Environment.ProcessorCount` inside each worker container report the full host core count instead of 1-2, which unlocked dramatically higher execution throughput (~90/sec → ~330+/sec) — but running many workers fully uncapped simultaneously (10+) reliably collapses the host (containers fail to start/answer, all schedule requests time out). The safer, repeatable middle ground found so far: cap workers at **2 CPU / 4GB each**, DB at **3 CPU / 6GB**, NATS at **3 CPU / 4GB** — gives most of the throughput benefit without the collapse risk. Next step: repeat this properly (3x-averaged, not single runs) on a dedicated machine with 20 workers once a suitably sized box is provisioned (a DigitalOcean CPU-Optimized droplet, 48 vCPU/96GB, ~$1.50/hr, was sized for this — 20 workers x 2 CPU = 40 vCPU + DB/NATS headroom).

## Messaging & Positioning
- **Standalone Mode is a Selling Point:** The documentation mentions that reverting from Distributed to Standalone is a one-way operation. We need to re-word this or handle it gracefully to prevent scaring off early adopters. Standalone is a huge benefit (start simple, scale to NATS later while keeping the code).
- **Fallback Bucket Starvation Penalty:** Add a limit or "never processed" flag for fallback buckets to prevent memory starvation on coordinator nodes if a user misconfigures lane priorities.
