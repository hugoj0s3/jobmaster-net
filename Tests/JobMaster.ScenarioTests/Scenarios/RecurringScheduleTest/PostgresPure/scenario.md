# RecurringScheduleTest.PostgresPure

Proves recurring schedules fire correctly end to end through a real Docker-container app
(`TargetTestRecurringApp`), covering all 4 combinations of **registration mode** (static, defined
at app startup vs. dynamic, created at runtime via HTTP) x **compiler** (`TimeSpanInterval` vs
`NaturalCron`). Unlike `Tests/JobMaster.IntegrationTests`'s equivalent `RecurringScheduleTest`
(seconds-level intervals, in-process handler, single process), this scenario uses a realistic
6-minute interval and asserts through Redis + the JobMaster API against a real containerized app,
matching the `ScheduleTest` suite's architecture.

## Topology

- `postgres-recurring` — one `TargetTestRecurringApp` container, standalone cluster
  (`Standalone: true`, master + worker in one process), database `PostgresRecurring`. A single
  cluster is enough here — unlike `ScheduleTest`'s `*Pure` scenarios, this isn't testing
  connection/drain lifecycle, just recurring-firing correctness.
- `api` — one `TargetTestApi` container, zero workers, registering the one cluster so the
  recurring-schedule cross-check (`GET /{clusterId}/recurring-schedules`) can be verified against
  the JobMaster API, not just Redis execution records.
- `dockerfilePath: "Tests/TargetTestRecurringApp/Dockerfile"` in `Phase1/postgres-recurring.json`
  is the actual point of this scenario existing: it's the first scenario to point a container at an
  app *other than* `TargetTestScheduleApp`/`TargetTestApi`, exercising `ScenarioRunner`'s
  now-real `ContainerDefinition.DockerfilePath`-driven image selection (previously dead JSON — see
  `ScenarioGlobalEnvironment.GetOrBuildAppImageAsync`).

## Why `TransientThreshold` is `00:10:00`, not the usual `00:02:00`

**Do not "fix" this back to 2 minutes.** `RecurringSchedulePlanner.PlanNextDates` floors its
planning horizon at `max(TransientThreshold, 5 minutes)` (`JobMasterConstants.DurationToLockRecords`,
hardcoded, not configurable). This scenario's 6-minute recurring interval means every candidate
occurrence is `CreatedAt + 6min` — if the horizon were only 5 minutes (what a 2-minute
`TransientThreshold`, floored, would give), every candidate would permanently exceed the horizon
and **the schedule would never fire a single occurrence**, not run slowly — it would silently stall
forever with `LastPlanCoverageUntil` advancing but nothing ever getting planned. `00:10:00` gives
~4 minutes of planning lead time ahead of each due date, comfortably robust.

## Why the interval is 6 minutes

Satisfies the "more than 5 minutes, less than 15 minutes, no seconds" requirement, clears the
5-minute planning floor above with margin, and divides 60 cleanly — avoiding an hour-boundary wrap
that would otherwise distort `NaturalCron`'s wall-clock-aligned firing pattern (see below) during
the ~17-minute observation window.

## Compiler behavioral difference (read this before touching the assertions)

- **`TimeSpanInterval`**: `GetNextOccurrence = cursor.Add(interval)` — *relative*. First fire is
  exactly `interval` after creation.
- **`NaturalCron "every 6 minutes"`**: cron-style, *wall-clock-aligned* — fires at fixed `:00/:06/
  :12/...` marks. First-fire delay after creation is variable (up to 6 minutes), not a fixed offset.
  Once firing has started, consecutive occurrences *are* exactly 6 minutes apart for both compilers
  — only the very first occurrence's timing differs between them.

`RecurringScheduleTestPhase1EmulatorBase` accounts for this: the "first execution near its expected
due time" check computes a different expected time per compiler (`CreatedAt + Interval` for
`TimeSpanInterval`, "next 6-minute wall-clock mark at or after CreatedAt" for `NaturalCron`), while
the "consecutive executions spaced ~6 minutes apart" check is shared between both.

## What Phase1 does

`PostgresPureTests.RunAllPhases` (via `PostgresPurePhase1Emulator` → the shared
`RecurringScheduleTestPhase1EmulatorBase`):

1. Creates 2 dynamic recurring schedules (`TimeSpanInterval`/`"00:06:00"`,
   `NaturalCron`/`"every 6 minutes"`) via `POST /recurring-schedule/tick` on the app container, each
   with a fresh `TestIdentifier`. The app also registers 2 *static* schedules at startup
   (`StaticTimeSpanIntervalProfile`, `StaticNaturalCronProfile` — fixed `TestIdentifier`s
   `"static-timespan-interval"`/`"static-natural-cron"`, duplicated as literals on this side since
   they're defined in a different process/project).
2. Asserts zero executions for all 4 within 20 seconds of creation/startup — catches an accidental
   "fires immediately" bug well before the real wait.
3. Waits (concurrently, not sequentially — ~17 minute budget) for each of the 4 to reach 2
   executions.
4. Asserts: no duplicate job IDs; first execution lands close to its expected due time (compiler-
   aware, see above); consecutive executions are spaced within ±10% of 6 minutes.
5. Cross-checks the 2 dynamic schedules against `GET /{clusterId}/recurring-schedules` —
   `ExpressionTypeId`/`Expression`/`JobDefinitionId` match.
6. Cancels the 2 dynamic schedules, waits 90 seconds (`CancelJobsFromRecurScheduleInactiveOrCanceledRunner`
   ticks every 30s), then asserts no more than one additional execution occurred after cancellation
   (an in-flight job at cancellation time isn't retroactively killed, only future occurrences are
   prevented). Static schedules are left running — they're simply torn down with the container.

Total expected runtime: ~20-25 minutes (dominated by the ~17-minute wait for 2 firings per
schedule), much shorter than `ScheduleTest`'s drain-lifecycle scenarios (~60-70 min).
