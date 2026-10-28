# ConfirmOnMasterTest — PostgresPure

Checks `ConfirmOnMasterAsync` end to end: a confirmed job or recurring schedule is on master as soon as the call returns, and the race with the agent's own save never runs anything twice or loses anything.

## Setup

One `pg-confirm` container runs `TargetTestScheduleApp` with one distributed Postgres cluster, `confirm-dist`:
- two Postgres agent connections (`ConfirmAgent1`, `ConfirmAgent2`), one worker each;
- so scheduling hands items to an agent connection and receipts are `AcceptedOnTransport`, which is the only case where confirm does anything.

The app confirms when the request sets `ConfirmOnMaster`: `Advanced.ConfirmOnMasterAsync(receipts)` for a batch of one-off jobs, `receipt.ConfirmOnMasterAsync()` for a recurring schedule. It returns how many receipts were `AcceptedOnTransport`.

The `api` container sees `confirm-dist`.

## Phase 1

1. **One-off jobs:** schedule 300 `fast` jobs and confirm them as a batch in the same request.
   - At least one receipt is `AcceptedOnTransport`, so the confirm had work to do.
   - Right after the request returns, all 300 are on master.
   - All 300 run, each exactly once.
2. **Recurring schedule:** create a `TimeSpanInterval` `00:00:15` schedule and confirm it.
   - Its receipt is `AcceptedOnTransport`.
   - Right after the request returns, it's on master.
   - It fires at least 3 times, one distinct job per occurrence. This also covers the agent's save hitting the duplicate: it must still plan the schedule, not get stuck.
   - Then it's cancelled.
3. **Settle:** after 5 seconds, still exactly 300 executions with unique job ids, and all 300 jobs reach `Succeeded` on master.
