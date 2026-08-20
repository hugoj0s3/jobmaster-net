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
Message and Job static indexes both in place, see `RavenDbMessageIndexes`/`RavenDbJobIndexes`)
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

## Running the full `IntegrationTests` project unfiltered throws `Cluster ID 'RT-Postgres-1' already exists`

Raised 2026-08-21 while running the complete test suite (Unit + Integration + Scenario, no filters) ahead
of the RavenDB provider's first release. Not a RavenDB issue -- 0 of the 390 failures were RavenDB tests
(RavenDB's own conformance suite passed 142/142, reproduced on a clean re-run). Every failure was in the
Postgres/MySql/SqlServer RepoConformance suite.

`RepoConformanceBootstrap` (`Tests/JobMaster.IntegrationTests/Fixtures/RepoConformance/RepoConformanceBootstrap.cs`)
already guards against concurrent double-registration within itself via a `SemaphoreSlim` + double-checked
`started` flag -- that part works. But `JobMasterClusterConnectionConfig.Create(...)` still throws
"Cluster ID 'RT-Postgres-1' already exists" when the whole `IntegrationTests` assembly runs unfiltered,
because xUnit runs independent collections (RepoConformance, RavenDb's own `RavenDbRepoConformanceCollection`,
and whatever collections back the Recurring/Scheduler/DrainMode test types) in parallel by default, and
something outside `RepoConformanceBootstrap`'s own lock registers the same hardcoded cluster ID a second
time from a different collection. Reproduced deterministically twice (390 failed/142 passed both times),
so not a flake.

This has plausibly always been latent -- `scripts-utils/run-integration-tests.sh` never runs the project
unfiltered, it always scopes to one `DB=$DB&TestType=X` combination per `dotnet test` invocation, which
happens to avoid ever triggering the cross-collection race. Nobody had run the whole assembly in one
process before. Needs a proper fix (either give every hardcoded test cluster ID a per-collection-unique
suffix, or synchronize registration across collections, not just within `RepoConformanceBootstrap`) --
deferred since it's a test-infrastructure gap unrelated to any single provider, not something to bundle
into the RavenDB-scoped PR that surfaced it.

## `TryGetApplicableIndexName` (Jobs) treats `Statuses` (plural) and `Status` (singular) inconsistently

Raised 2026-08-21 during PR review of `RavenDbMasterJobsRepository.TryGetApplicableIndexName`. The single
deployed static index (`RavenDbJobIndexes.ByClusterStatusLockNextPlanName`) maps a `Status` field, and the
method includes `StatusField` in its required-prefix tokens (making the index eligible) whenever
`criteria.Status.HasValue` -- but `criteria.Statuses.Count > 0` (the plural "in" filter) unconditionally
returns `null` instead, disqualifying the index entirely rather than also contributing a `StatusField`
token. There's no obvious RQL-level reason for the difference: `e.Status in ($statuses)` against an
indexed field is just as servable by a static index as `e.Status = $status` -- RavenDB's inverted-index
model doesn't distinguish equality from an `in` clause the way a B-tree composite key might.

Not fixed here since it wasn't verified whether this is a real gap or effectively moot: `TryGetApplicableIndexName`
is only called from `AcquireAndFetchAsync`'s candidate query and `ProbeForAcquireAsync`, and it's unclear
whether either of those two call sites is ever actually invoked with `Statuses` (plural) set in practice --
if not, this branch is defensive completeness for a `JobQueryCriteria` shape these two callers never
produce, not a real missed optimization. Worth checking actual caller usage before deciding whether to
extend the token-building logic to cover `Statuses` too.

## `TryGetApplicableIndexName` hardcodes the PartitionLockId/PartitionLockExpiresAt assumption instead of taking it as a parameter

Raised 2026-08-21 during the same PR review. `TryGetApplicableIndexName` unconditionally adds
`PartitionLockIdField`/`PartitionLockExpiresAtField` to its required-prefix tokens, justified only by a
comment ("every caller (AcquireAndFetchAsync, ProbeForAcquireAsync) only ever queries isLocked: false
candidates") -- an unenforced, comment-only invariant rather than something the method actually verifies.
`BuildWhereRql`, right next to it in the same file, already takes an explicit `bool? isLocked` parameter
for exactly this -- `TryGetApplicableIndexName` should take the same kind of explicit flag instead of
silently assuming every future caller will keep filtering on the partition lock fields. If a future call
site ever calls this method without that filter, the method would still silently build a required prefix
including those two fields, understating what the index is being asked to cover -- the kind of drift the
codebase's own "hard invariants need explicit enforcement, not just comments" convention exists to catch.

Deferred (reminder only, not fixed) per explicit request during PR review -- not touched this close to
publishing. Fix would be: give `TryGetApplicableIndexName` an `isLocked` parameter matching `BuildWhereRql`'s,
and pass it explicitly from both call sites instead of relying on the comment.

## Future: a second Jobs static index without the partition-lock fields will require `TryGetApplicableIndexName` to become a real index-selection mechanism, not a single-index check

Raised 2026-08-21 (user's own forward-looking note during the same PR review, right after the two
`TryGetApplicableIndexName` items above). `RavenDbJobIndexes` currently deploys exactly one static index
(`ByClusterStatusLockNextPlanName`, mapping ClusterId/Status/PartitionLockId/PartitionLockExpiresAt/
NextPlanExecutionAt), and `TryGetApplicableIndexName`'s whole design assumes there's only ever this one
index to check against -- it unconditionally builds a required-prefix that always includes
PartitionLockId/PartitionLockExpiresAt (see the item above) and checks it via `StartsWith` against
`DeployedIndexNames` (currently a 1-element list).

The user is considering adding a second static index that does NOT map the partition-lock fields at all,
for query shapes that don't filter on lock state. Once more than one index exists, the current
"build one required-prefix string, StartsWith-match it against whatever's deployed" approach stops being
sufficient by itself -- `TryGetApplicableIndexName` needs to become genuine index *selection* among
multiple candidate indexes with different field coverage (e.g. picking the narrowest/cheapest index that
still covers a given criteria's fields), not just a yes/no check against one hardcoded shape. This is the
same underlying gap as the isLocked-parameter item above -- both stem from the method's field-inclusion
logic being hardcoded to match today's single index rather than being driven generically by what the
actual deployed index set covers. Worth designing both together whenever this gets picked up, rather than
patching the isLocked assumption first and then redoing it again once a second index shows up.

## No test (any provider) ever configures master and agent on genuinely separate databases

Raised 2026-08-21 while investigating whether the RavenDB fallback-bucket scenario's Coordinator-only
setup could mask an index-deployment timing gap (see `RavenDbJobMasterRuntimeSetup.OnBeforeStartAsync`
investigation, same review). Checked and confirmed this is **not** RavenDB-specific -- it's the same
across every provider:

- `RepoConformanceBootstrap.cs`: `cfg.UsePostgresForMaster(postgres.GetConnectionString())` then
  `cfg.AddAgentConnectionConfig(...).UsePostgresForAgent(postgres.GetConnectionString())` -- literally
  the same connection string for master and agent. Identical pattern for MySql and SqlServer in the same
  file, and for RavenDB in `RavenDbRepositoryFixture.cs` (`cfg.UseRavenDb(connectionString)` for both).
- Every `FallbackBucketTest/*Pure` scenario variant (Postgres/MySql/SqlServer/RavenDb) configures only
  one `ConnectionString`, no separate `AgentConnections` at all -- expected there specifically, since
  fallback is defined to always reuse the master's own connection regardless of provider, not a gap on
  its own.

But outside the fallback-specific case, there is currently **no integration or scenario test, for any
provider, that ever exercises master and an agent connection pointing at genuinely different database
instances** -- the realistic production topology once a cluster has real Coordinator + Execution workers
rather than everything colocated. Same-database test setups can accidentally paper over bugs that only
manifest when master-side and agent-side state are truly independent (e.g. a resource -- index, schema,
whatever -- that only got provisioned on "the one database" because it happened to double as both roles
in the test, not because the provisioning path actually covers agent connections correctly on their own).
Worth adding at least one conformance/scenario configuration per provider with master and agent on
distinct database instances, to close this blind spot -- broader than any single provider's PR, so not
attempted here.
