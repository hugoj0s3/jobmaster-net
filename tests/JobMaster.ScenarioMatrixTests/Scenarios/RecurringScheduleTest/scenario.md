# RecurringScheduleTest

Recurring schedules fire on time and exactly once per occurrence, across 8 registration / compiler combinations, on
a real `TargetTestRecurringApp` container. Replaces the earlier per-provider suite's `RecurringScheduleTest`
(6 per-provider copies there), with stronger per-execution checks.

## Topology
One container `mx-<flavor>-recurring`, one **distributed** cluster of the same name: two agent connections
(`agent-1`, `agent-2`, the flavor's agent provider; on HybridNatsRavenDb that covers both directions), one worker each.
`TransientThreshold` 5 min (NATS caps it there). The planner dispatches a schedule's next occurrence on its first
planning attempt even when it's beyond the horizon, so a 6 min interval doesn't stall at 5 min.

The earlier suite's Pure variants used a standalone cluster instead (10 min threshold); one template can't be both,
so the matrix covers recurring schedules on a distributed cluster only.

## Combinations (all every 6 min)
| Combination | Registration | Compiler | Fires relative to |
|---|---|---|---|
| `static-timespan-interval` | static profile | TimeSpanInterval | creation |
| `static-natural-cron` | static profile | NaturalCron | creation (NaturalCron isn't grid-aligned) |
| `attribute-static-tick` | `[TimeSpanIntervalSchedule]`, no profile | TimeSpanInterval | creation |
| dynamic TimeSpanInterval | `RecurringAsync` | TimeSpanInterval | creation |
| dynamic NaturalCron | `RecurringAsync` | NaturalCron | creation |
| dynamic Advanced | `Advanced.RecurringAsync<TDefinition>()` | TimeSpanInterval | creation |
| `static-cronos` | static profile (`AddJobMasterCronos`) | Cronos `*/6 * * * *` | the :x6 wall-clock grid |
| `static-ncrontab` | static profile (`AddJobMasterNCrontab`) | NCrontab `*/6 * * * *` | the :x6 wall-clock grid |

## Flow
1. Create the three dynamic schedules (the static ones were registered at startup).
2. After 20 s, nothing creation-relative has fired yet.
3. Wait (17 min max) for ≥ 2 executions of each, then check per execution, from the job's `ScheduledAt`:
   - each `ScheduledAt` appears once (twice = duplicate planning), and no job runs twice;
   - consecutive `ScheduledAt` are exactly 6 min apart (none skipped, none extra);
   - each job runs within 60 s after its `ScheduledAt` (and not more than 5 s before);
   - creation-relative: the first `ScheduledAt` is `CreatedAt + 6 min` (±1 min);
   - grid-aligned: every `ScheduledAt` is on a :x6 mark, and the first is no later than the first :x6 mark after
     start (a missed first slot fails clearly).
4. The dynamic schedules are visible through the API with the right expression, type and job definition.
5. Cancel the dynamic schedules; 90 s later each has at most one more execution (one already in flight).

## Flavors
All six.
