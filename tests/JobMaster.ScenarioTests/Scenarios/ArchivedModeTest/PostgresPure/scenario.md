# Archived mode

This scenario checks the two jobs an archived cluster has:

1. **Keep history.** Finished jobs and schedules from an active cluster are copied to the archive cluster once
   their retention time has passed, and removed from the active cluster. Nothing runs on the archive.
2. **Don't lose late work.** A job or schedule that reaches a cluster just after it was archived is moved to the
   active cluster it points to, and runs there.

The run is split into three phases. Phase 1 covers the first goal, phases 2 and 3 the second. Phases 2 and 3
currently exist only for Postgres; the other storage providers run phase 1 only.

## Phase 1: finished jobs are archived

**Setup.** One app with two clusters:

| Cluster | Mode | Role |
|---|---|---|
| `archive-source` | Active | An ordinary cluster that runs jobs. Its history is kept for 10 minutes (the shortest allowed), then sent to `archive-target`. |
| `archive-target` | Archived | Only stores history. |

**Steps.**
1. Schedule 200 jobs on `archive-source`. 199 succeed; 1 is made to fail on purpose, with no retries, so a failed
   job is archived too, not only successful ones.
2. Create a recurring schedule and cancel it right away, so it's finished and can be archived as well.
3. Wait until all 200 jobs and the schedule have left `archive-source` (the retention time, plus up to 5 minutes for
   the cleanup to notice).

**Expected result.** `archive-target` holds the same 200 jobs and the schedule:
- same ids;
- same final status (199 succeeded, 1 failed, schedule cancelled);
- their execution history and the failed job's error log came along.

`archive-source` no longer has any of them.

## Phases 2 and 3: late work on an archived cluster

**Why.** A cluster can be switched to Archived while a job is being scheduled on it. The job can then end up on the
archived cluster, where nothing runs it. With a target set, the archived cluster moves such jobs and schedules to
the target, an active cluster, every 2.5 minutes.

**Setup.** One app with two clusters:

| Cluster | Phase 2 | Phase 3 |
|---|---|---|
| `stray-old` | Migrating to `stray-new`, but no workers running | Archived, target `stray-new`, one coordinator running |
| `stray-new` | Active, no workers running | Active, with a worker that runs jobs |

**Phase 2: leave late work behind.**
1. Schedule 20 jobs and one recurring schedule (every 15 seconds) on `stray-old`. No worker runs there, so they
   stay put.
2. Stop the app and mark `stray-old` as Archived directly in its database. This recreates what the race leaves
   behind: the jobs got in, then the cluster became Archived.

**Phase 3: the archived cluster hands the work over.**
1. Start the app again with `stray-old` as Archived, pointing to `stray-new`. Startup must accept it even though
   unfinished jobs are still there, because they have somewhere to go.
2. Wait until `stray-old` is empty.

**Expected result.**
- All 20 jobs ran on `stray-new`, each exactly once, with the same ids.
- The recurring schedule is on `stray-new`, still active, and runs there.

## Not covered here

- An archived cluster with no target, or with a target this app doesn't know: nothing is moved and a critical error
  is logged. Covered by unit tests.
- Jobs that were already handed to a worker that then died: they're moved once their processing deadline (10
  minutes) passes. Too slow to stage here.
- An archive cluster that can't be reached during cleanup: history is deleted without being copied, and a critical
  error is logged.

## When it fails

If phase 1's jobs don't all run in time, the test writes a report on the jobs that never ran and names the file in
the failure message. It holds their status, executions and logs, the workers, the app's output and, for Postgres,
the raw database rows. On 2026-10-31 that report showed a test-app problem (its Redis connection wasn't ready for the
first burst of jobs), which was then fixed.

## Timing

All waits use JobMaster's real timings, nothing is shortened. Phase 1 takes about 12–18 minutes, phases 2 and 3 about
5–10 more. The full run took 27 minutes on 2026-10-31.

Each run uses fresh test ids, because all storage-provider variants share one Redis and could otherwise see each
other's results.
