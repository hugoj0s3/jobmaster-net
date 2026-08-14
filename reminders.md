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

## `IJobMasterRuntimeSetup.OnAfterStartedAsync` — future symmetric hook

`OnStartingAsync` was renamed to `OnBeforeStartAsync` (interface + all implementers: `SqlJobMasterRuntimeSetup`,
`PostgresJobMasterRuntimeSetup`, `NatsJetStreamJobMasterRuntimeSetup`, `DefaultRuntimeValidatorSetup`) to make
room for a future `OnAfterStartedAsync` counterpart — a hook that runs once the runtime has fully started
(workers created and started, `Started = true`), symmetric to `OnBeforeStartAsync` running before any of that.
Not implemented yet; just the naming groundwork.
