# Reminders

Follow-ups noted during work but deliberately deferred out of the PR they came up in.

## `JobMasterRuntime.StartAsync` validation is still split across two places

Raised 2026-07-19 while working on the Migrating-mode PR. Point 1 (validation must fully complete
before any `OnBeforeStartAsync` side effects run) was fixed as part of that PR — `PreValidation()` now
runs right after the `ValidateAsync` loop and before `OnBeforeStartAsync`.

Point 2, not yet addressed: some validation still happens *after* that point, inside the per-cluster
loop in `StartAsync` — most notably the agent-connection fingerprint check
(`existingConnection.Fingerprint != fingerprint` + `ProtectConnectionChanges` → throws) around
`JobMaster\Sdk\Background\JobMasterRuntime.cs`. This check is interleaved with real side-effecting work
in the same loop (saving connections, merging cluster config, persisting to DB), so a fingerprint
mismatch on a later cluster in the loop can throw *after* earlier clusters in the same startup have
already had connections saved / config persisted — an inconsistent partial-startup state, the same class
of problem Point 1 fixed, just not yet untangled here.

Properly fixing this means splitting the per-cluster loop into a validate-everything pass and a
separate apply-everything pass — bigger and riskier than Point 1's reorder (connection registration,
fingerprinting, and config persistence are currently one intertwined loop), and not a natural side
effect of whatever feature happens to touch `JobMasterRuntime` next. Worth its own dedicated
investigation/PR rather than opportunistic bundling.

## `ConfigFromJson` — `RepoType` comparisons are case-sensitive, inconsistent with the rest of the surface

Raised 2026-08-14 while checking `ConfigFromJson`'s case-sensitivity end to end. JSON property names
(`PropertyNameCaseInsensitive` on the `string`/`Stream` overloads, `IConfiguration.Get<T>()`'s inherent
case-insensitivity on the `IConfiguration` overload) and every enum-like string value (`ClusterMode`,
`DisabledPriorities`, `WorkerMode`, `BucketQtyConfig` keys — all parsed via
`Enum.TryParse(value, ignoreCase: true, ...)`) are deliberately case-insensitive, with test coverage to
match.

`RepoType` (`ClusterRepoType`/`AgentRepoType` — `"Postgres"`/`"MySql"`/`"SqlServer"`/`"NatsJetStream"`) is
the one exception, and it doesn't look deliberate: `JobMasterIocRegistrationAttribute
.RegisterProviderExtensionsForMaster`/`RegisterProviderExtensionsForAgent` resolve the provider module via
a plain `repoTypeProp.GetValue(null)?.ToString() != repositoryType` (ordinal comparison, no
`ignoreCase`), and `ConnectionOptionsBinderFactory`'s repo-type dictionary is built via
`.ToDictionary(s => s.RepoType, s => s)` with no `StringComparer.OrdinalIgnoreCase` either. A lowercase
`"postgres"` in a JSON config silently matches no registered provider instead of raising a clear
validation error — the failure only surfaces later as a confusing missing-implementation error, not an
obvious "invalid RepoType" message at config time.

Fix should make repo-type comparisons case-insensitive throughout (both spots above), matching the rest
of `ConfigFromJson`'s design. This was found by spot-checking one specific field, not a systematic
sweep — worth auditing every other enum-like/identifier string this surface parses to confirm nothing
else was missed the same way before considering this done.

## No shared `Logs` RepoConformance suite for any provider

Raised 2026-08-15 while implementing `IMasterLogsRepository` for the RavenDB provider. Unlike the other
4 Master interfaces (Jobs, RecurringSchedules, GenericRecords, DistributedLocker), `RepositoryFixtureBase`
(`Tests/JobMaster.IntegrationTests/Fixtures/RepoConformance/RepositoryFixtureBase.cs`) has no `MasterLogs`
slot, and there is no shared `RepositoryLogsConformanceTests<TFixture>` base class the way
`RepositoryDistributedLockerConformanceTests`/`RepositoryGenericRecordsConformanceTests` exist. This means
SQL's `SqlMasterLogsRepository`/provider overrides have never had cross-provider conformance coverage —
a pre-existing gap, not something introduced by the RavenDB work.

Because of this, RavenDB's Logs implementation shipped with its own standalone test class
(`RavenDbLogsRepositoryConformanceTests`, 6 tests against a concrete `RavenDbRepositoryFixture`, not a
shared generic base) rather than the reused-shared-base pattern the other 4 interfaces followed. Properly
fixing this means adding `MasterLogs` to `RepositoryFixtureBase`, writing a shared
`RepositoryLogsConformanceTests<TFixture>` base class, and wiring it into all 4 provider fixtures
(Postgres/MySql/SqlServer/RavenDb) — bigger than the RavenDB increment it was found in, and touches SQL
provider test files outside that scope, so deferred rather than opportunistically bundled in.

## `IJobMasterRuntimeSetup.OnAfterStartedAsync` — future symmetric hook

`OnStartingAsync` was renamed to `OnBeforeStartAsync` (interface + all implementers: `SqlJobMasterRuntimeSetup`,
`PostgresJobMasterRuntimeSetup`, `NatsJetStreamJobMasterRuntimeSetup`, `DefaultRuntimeValidatorSetup`) to make
room for a future `OnAfterStartedAsync` counterpart — a hook that runs once the runtime has fully started
(workers created and started, `Started = true`), symmetric to `OnBeforeStartAsync` running before any of that.
Not implemented yet; just the naming groundwork.

## RavenDB's `RuntimeDbOperationLimit` defaults (50 cluster / 25 agent) are unvalidated guesses

Raised 2026-08-19 while benchmarking the RavenDB provider (`JobMaster.RavenDb/RavenDbJobMasterRuntimeSetup.cs`).
These values were carried over from Postgres/SqlServer's own defaults on the reasoning that RavenDB is
also a remote HTTP-accessed database server, not because they were actually tuned for RavenDB's specific
concurrency/throughput profile. A benchmark comparison at burst-1000/3-workers scale (with RavenDB's
Message and Job static indexes both in place, see `RavenDbMessageIndexDefinitions`/`RavenDbJobIndexDefinitions`)
found no measurable difference between 10/5 and 50/25 — meaning that comparison didn't actually validate
50/25 as correct, only that neither value was the bottleneck at that specific scale. Whether 50/25 holds
up at higher concurrency (larger worker counts, sustained paced load rather than a single burst) is still
untested. Worth a dedicated tuning pass in a future version — sweep a wider range of values against both
burst and paced tiers at larger scale, rather than assuming the SQL-derived defaults are actually right
for RavenDB.

## `AssignJobsToBucketsRunner`'s imminent-path lock caps claim throughput at one `TransferBatchSize` per 10s window

Raised 2026-08-19 while benchmarking the RavenDB provider's `AcquireAndFetchAsync` at larger burst sizes
(`JobMaster/Sdk/Background/Runners/JobAndRecurringScheduleLifeCycleControl/AssignJobsToBucketsRunner.cs`).
Core SDK behavior, not RavenDB-specific -- applies identically to every provider.

`BucketAssignerImminentLock` is a time-bucketed distributed lock keyed by a 10-second window
(`ProbeWindowInSeconds`); only the one coordinator instance that wins it does anything that tick, every
other coordinator instance just sees the lock held and skips. Combined with `TransferBatchSize` (default
1000), this means a backlog bigger than one batch needs multiple ~10-second-apart cycles to fully drain,
regardless of how many coordinator instances are configured or how fast the DB itself responds -- at
burst-5000 scale this alone plausibly accounts for a large share of both SQL Server's and RavenDB's
measured execution latency (SQL Server's ~41s Immediate mean at that scale roughly matches "4-5 cadence
cycles," suggesting its own per-cycle DB cost is small relative to the cadence gate itself).

The lock exists for a real reason, not defensively: without it, N concurrent coordinators would each
independently query the same "top TransferBatchSize unlocked, ordered by NextPlanExecutionAt asc,
Offset=0" candidate set and race to claim it. The race itself is safe (RavenDB/SQL both resolve it
correctly per-document/per-row, no double-claim), but it's wasteful -- whichever coordinators lose the
race paid for a full acquire round trip (query + patch/update + confirm) for few or zero rows, since
every concurrent query targets the identical front-of-queue window rather than different slices of the
backlog.

Three possible future directions discussed, none implemented (all deferred out of the RavenDB-scoped
PR this was raised in, and all are core-SDK changes affecting every provider):

1. **Partition candidates across concurrent coordinators** (different offsets, hash-based sharding, or
   similar) so they target genuinely different slices of the backlog instead of racing for the same
   window. Would remove the waste properly, but is a real redesign of the claim path, not a small tweak.
2. **Over-fetch and randomly thin the candidate list** -- e.g. query `TransferBatchSize * 1.5` candidates
   and randomly drop back down to `TransferBatchSize` before attempting to claim, so two coordinators
   racing at the same instant end up attempting different (though still overlapping) subsets by chance.
   Considered and set aside (2026-08-19, user's own assessment: "too odd") -- also weak on the math: for
   two independent random `k`-out-of-`N` draws, expected overlap is `k²/N` -- at `k=1000, N=1500` that's
   ~667, meaning ~67% of each coordinator's claimed candidates would still collide on average. Reduces
   wasted work, doesn't come close to eliminating it, and adds an odd "randomly discard rows you just
   fetched" step to the claim path for a small payoff.
3. **Add jitter to the coordinators' acquire-attempt cadence instead** (user's preferred direction,
   2026-08-19) -- rather than reshaping which candidates get claimed, randomize *when* each coordinator
   instance attempts the lock/probe cycle (e.g. a small random offset added to the polling interval per
   instance) so concurrent coordinators don't all hit the lock at the same instant every cycle. Doesn't
   change how many jobs get claimed per window (still capped by `TransferBatchSize`, still one winner per
   10s bucket), but should reduce simultaneous lock-contention spikes -- the wasted round trips come from
   many coordinators showing up to attempt the lock at the exact same moment, not from the lock itself.
   Lower-risk and more conventional than option 2 (standard thundering-herd mitigation, no change to
   claim/candidate semantics at all, just polling timing) -- the more promising direction to prototype
   first if this gets picked up.
