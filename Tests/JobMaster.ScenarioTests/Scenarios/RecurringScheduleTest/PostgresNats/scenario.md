# RecurringScheduleTest.PostgresNats

Same as `RecurringScheduleTest.PostgresPure` (see that scenario's `scenario.md` for the base
rationale — interval choice, compiler timing differences, assertions), but Postgres master + one
NATS JetStream agent connection instead of standalone Postgres. Mirrors why
`ScheduleTest.PostgresNats` has no standalone leg: NATS can only ever be an agent connection, never
a cluster's own master storage.

## Why `TransientThreshold` is `00:05:00` here, not `00:10:00` like the other 3 variants

`NatsJetStreamConstants.MaxThreshold` hard-caps `TransientThreshold` at 5 minutes for any cluster
with a NATS agent connection — `NatsJetStreamJobMasterRuntimeSetup.ValidateAsync` rejects startup
above that. But `RecurringSchedulePlanner`'s planning horizon is `max(TransientThreshold, 5
minutes)` (the 5-minute floor is hardcoded, not configurable) — so on *this* cluster the horizon is
**always exactly 5 minutes**, strictly less than the 6-minute recurring interval every variant uses.

This does not mean the schedule stalls forever the way the original `TransientThreshold: 00:02:00`
mistake did in `PostgresPure` (floored to 5min, permanently < interval, on *every* pass). Here, the
first planning pass can't land the 6-minute candidate inside a 5-minute horizon either, but a
*later* replanning pass — once real time has advanced enough that `now + 5min >= candidateDate` —
will. So the schedule still fires, just with its first occurrence potentially landing later than a
naive `CreatedAt + Interval` estimate would predict, by however long it takes an extra replanning
cycle to catch up. `PostgresNatsPhase1Emulator` widens `WaitForTwoFiringsTimeout` (24 min, up from
17) and `FirstFiringLateTolerance` (9 min, up from 5) to absorb this — calibrated the same way the
`PostgresPure` static-schedule tolerance was: implement, run, observe the actual gap, adjust if the
run shows it's still too tight.

This is a structural NATS constraint, not a bug — don't "fix" it by lowering the interval below 5
minutes or raising `TransientThreshold` above 5 minutes; both are hard requirements from elsewhere
(the interval range and NATS's own cap, respectively).
