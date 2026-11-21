# StandaloneToDistributedTest

Moving a cluster off standalone loses nothing: a standalone worker stops right after scheduling, and a distributed
topology takes over the same cluster and master database and runs every job. Replaces
the earlier per-provider suite's `StandaloneToDistributedTest` (5 per-provider copies there).

## Variants
Every flavor × both stop types (`ScenarioStopType`), 12 runs: **SigTerm** (the standalone worker's shutdown runs:
buckets handed over, buffered jobs flushed back) and **Crash** (SIGKILL: nothing of it runs). Each run has its own
cluster and master database, `mx-<flavor>-<sig|crash>-s2d`.

## Phases
- **Phase 1:** one `…-s2d-standalone` container, a standalone cluster (no agent connection; the master database
  does both). Schedule 300 `fast` jobs, then stop it at once with the run's stop type, without waiting for any job
  to settle: phase 2 has to recover jobs caught at every stage (pending save, on master, in a bucket, running).
- **Phase 2:** the same cluster id and master database, now distributed: a `…-s2d-coordinator` container
  (coordinator) and a `…-s2d-executor` container (execution worker on a new agent connection `agent-1`, the
  flavor's agent provider). `TransientThreshold` 2 min (NATS caps it at 5). Recovery uses two real, unmodified paths:
  - **Fast:** at startup JobMaster finds buckets still owned by the standalone worker and adds a `StandaloneDrainer`
    (Drain mode) on the reserved standalone connection (same connection string as master). `MarkBucketAsLostRunner`
    (2.5 min tick) marks the orphaned bucket Lost, `AssignedLostBucketsRunner` (1 min tick) gives it to that drainer.
  - **Backstop:** a job still in a bucket the drain missed is reclaimed by `HeldOnMasterDeadlineTimeoutJobsRunner`
    once its `ProcessDeadline` (10 min from assignment) passes while its bucket isn't Active/Completing.

  Either way, back on master, `AssignJobsToBucketsRunner` assigns it to the new bucket on `agent-1`. All 300 are
  `Succeeded` on master with their ids (20 min max), every one ran, and `agent-1` is alive. A repeated job runs at most
  twice, on at most 5% of the jobs (Crash: 3 times, 10%): one stopped after its handler but before its result was saved stays
  `Processing` until the deadline runner reassigns it (at least once, not exactly once; see DrainModeTest).

## Things to know
- **Crash runs:** a crashed standalone worker hands nothing over, so recovery waits for dead-worker detection and,
  for leased pending saves, the lease to end (90 s–10 min). The 20 min timeout already covers the 10 min backstop.
