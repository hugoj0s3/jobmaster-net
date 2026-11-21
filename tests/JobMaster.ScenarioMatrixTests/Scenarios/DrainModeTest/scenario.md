# DrainModeTest

No job is lost when every executor stops right after a large schedule, with jobs caught at every stage
of the pipeline. Each worker mode (`Coordinator`, `Execution`, `Drain`) runs alone in its own container, one mode
change per phase, under real load. Replaces the earlier per-provider suite's `DrainModeTest` (5 per-provider
copies there); the design reasoning and the bugs it found are in their `scenario.md` files (git history).

## Variants
Every flavor × both stop types (`ScenarioStopType`), 12 runs:
- **SigTerm** (`docker stop`): the executors' own shutdown runs (buckets handed over as Lost, buffered jobs flushed
  back to master).
- **Crash** (SIGKILL): nothing of the shutdown runs; recovery comes only from the other workers (dead-worker
  detection, lease expiry on the agent, processing deadlines).

Each run has its own cluster and master database, `mx-<flavor>-<sig|crash>-drain`, and its own test identifiers
(every run shares one Redis), so runs never see each other's jobs.

## Topology
One cluster, 3 agent connections (`agent-1/2/3`, the flavor's agent provider), one worker per connection:

| Phase | Containers | Modes |
|---|---|---|
| 1 | coordinator, executor-1/2/3 | Coordinator, Execution ×3 (`ParallelismFactor` 4) |
| 2 | coordinator, drainer-1/2/3 | Coordinator, Drain ×3 |
| 3 | coordinator, executor-1/2/3 | Coordinator, Execution ×3 |

Each connection keeps its identity: executor-N stops, drainer-N (same `agent-N`) inherits its buckets, executor-N
returns on the same connection. The runner never stops a container a later phase leaves out, so phase 1 stops the
executors explicitly, and the drainers keep running through phase 3. The coordinator is restarted with each phase.
`TransientThreshold` is 5 min (NATS caps it there; every job is due now, so it changes nothing for the others).

## Job plan
5000 jobs (`JM_DRAIN_JOBS_MULTIPLIER` scales it): 3500 `fast` (Medium), 1250 `normal` (High), 250 `slow` (Low), from
`DrainModeTestPlan`. Above `TransferBatchSize` (1000), so onboarding takes several transfer cycles. Each batch is
split evenly over executor-1/2/3, round-robin, in requests of at most 3500 jobs (the app schedules a request's jobs
concurrently; the HTTP call times out at 100 s). Each executor declares only its own agent connection, so this is
what gives every connection pending saves to drain; the earlier suite scheduled everything through executor-1.

## Phases
- **Phase 1:** schedule everything, then stop all 3 executors at once with the run's stop type. No waiting first:
  jobs are caught at every stage (pending save on the agent, on master, in a bucket, running), so phase 2 needs
  both drain runners (save pending and processing), not just one.
- **Phase 2:** capture phase 1's bucket ids, then wait (10 min max) until every job is on master, whatever its
  status: none lost by the stop. Bucket cleanup isn't waited for here.
- **Phase 3:** the executors return. Every job reaches `Succeeded` on master (45 min max), and per batch the
  Redis-tracked executions cover exactly the API's `Succeeded` jobs: none lost. A repeated job runs at most twice, on at most 5%
  of the batch (Crash: 3 times, 10%) (see Things to know). A failure report is
  written (`%TEMP%/jm-scenario-diag/drainmode-failures-<cluster>.txt`, not asserted). Last, each of phase 1's
  buckets reaches `ReadyToDelete` or is already gone (20 min max, checked by captured id, so the new executors'
  buckets don't count).

## Things to know
- **A job can run twice.** An executor stopped after a handler finished but before its `Succeeded` reached master
  leaves the job `Processing`; once its `ProcessDeadline` passes, `HeldOnMasterDeadlineTimeoutJobsRunner` puts it
  back on master and it runs again on a new bucket (at least once, not exactly once). Seen on Crash runs (1–2 jobs of
  5000, GitHub Actions, 2026-11-21); allowed on both stop types, more on Crash (a SIGKILL leaves the window to every running job, SigTerm only to
  those the shutdown can't finish saving). That's also why the wait is on master's `Succeeded`
  count, not on the execution count: a repeat can make up the count while another job is still waiting for its deadline.
- **A coordinator still runs jobs during phase 2.** When a job finds no bucket for 2.5 min
  (`NoBucketFallbackThreshold`), the coordinator creates a fallback bucket and runs it itself, one job at a time. At
  this volume it always triggers, so phase 2 asserts nothing about job status; finishing everything is phase 3's job.
- **The bucket lifecycle is checked only up to `ReadyToDelete`.** `Draining → ReadyToDelete` and
  `ReadyToDelete → destroyed` each wait about 10–20 min (`BucketNoJobsBeforeReadyToDelete`); destruction is covered
  by `DestroyReadyToDeleteBucketsRunnerTests`.
- **Crash runs recover more slowly:** a crashed executor hands nothing over, so its buckets wait for dead-worker
  detection and its leased pending saves for the lease to end (90 s–10 min). The timeouts are the earlier suite's,
  tuned for the SigTerm path; widen them only once a Crash run shows it's needed.

Run one variant: `dotnet test tests/JobMaster.ScenarioMatrixTests --filter "FullyQualifiedName~DrainModeTest&DisplayName~PostgresPure&DisplayName~Crash"`.
