# ConfirmOnMasterTest

Checks `ConfirmOnMasterAsync` and receipt / by-id cancel end to end: a confirmed job or recurring schedule is on master
as soon as the call returns, the race with the agent's own save never runs anything twice or loses anything, and a
cancel that returned true never runs. Matrix port of `JobMaster.ScenarioTests/Scenarios/ConfirmOnMasterTest`
(Postgres only there).

## Setup
One `mx-<flavor>-confirm` container runs `TargetTestScheduleApp` with one distributed cluster of the same name:
- two agent connections (`agent-1`, `agent-2`, the flavor's agent provider), one worker each;
- so scheduling hands items to an agent connection and receipts are `AcceptedOnTransport`, the only case where
  confirm does anything;
- `TransientThreshold` 2 min (NATS caps it at 5).

The app confirms when the request sets `ConfirmOnMaster`: `Advanced.ConfirmOnMasterAsync(receipts)` for a batch of
one-off jobs, `receipt.ConfirmOnMasterAsync()` for a recurring schedule. It returns how many receipts were
`AcceptedOnTransport`. The `api` container sees the cluster.

## Phase 1
1. **One-off jobs:** schedule 300 `fast` jobs and confirm them as a batch in the same request.
   - At least one receipt is `AcceptedOnTransport`, so the confirm had work to do.
   - Right after the request returns, all 300 are on master.
   - All 300 run, each exactly once.
2. **Recurring schedule:** create a `TimeSpanInterval` `00:00:15` schedule and confirm it.
   - Its receipt is `AcceptedOnTransport`, and right after the request returns it's on master.
   - It fires at least 3 times, one distinct job per occurrence (the agent's duplicate save must still let it be
     planned), then it's cancelled.
3. **Settle:** after 5 s, still exactly 300 executions with unique job ids, and all 300 reach `Succeeded` on master.
4. **Cancel:**
   - 50 jobs due in 2 min, cancelled from their receipts: every cancel succeeds.
   - 50 jobs due in 2 min, cancelled by id (the app retries until the agent saved them): every cancel succeeds.
   - 50 jobs due now, cancelled from their receipts: a cancel may lose the race (returns false, the job runs); the
     ones not cancelled run exactly once.
   - A recurring schedule cancelled from its receipt, and one cancelled by id once on master: both succeed.
   - Past the 2 min due time + 45 s: nothing cancelled ran, a job whose cancel returned true never ran, neither
     cancelled schedule fired, and master holds the jobs `Cancelled` and the schedules `Canceled`.

## Flavors
All six.
