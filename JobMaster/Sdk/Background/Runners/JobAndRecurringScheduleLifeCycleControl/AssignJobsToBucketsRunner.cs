using System.Collections.Concurrent;
using System.Diagnostics;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Runners.JobsExecution;
using JobMaster.Sdk.Background.ScanPlans;
using JobMaster.Sdk.Services.Master;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

/// <summary>
/// Acquires <c>OnMaster</c> jobs not yet past the transient threshold and assigns them to
/// the best available bucket, then dispatches each job to its target bucket. Uses a
/// two-tier probe/execute pattern: every <see cref="SucceedInterval"/> a cheap
/// <c>COUNT + MIN(NextPlanExecutionAt)</c> probe determines whether to run immediately
/// (imminent path, time-bucketed distributed lock) or defer to the scan-plan interval
/// (scan-plan path, slot-based distributed lock). Only runs when the cluster is in
/// <c>ClusterMode.Active</c>. If no bucket can be found for a job beyond
/// <c>NoBucketFallbackThreshold</c>, a temporary fallback bucket is created locally
/// to prevent job starvation.
/// </summary>
internal class AssignJobsToBucketsRunner : JobMasterRunner
{
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IMasterJobsService masterJobsService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly IMasterAgentWorkersService masterAgentWorkersService;
    private readonly IMasterClusterConfigurationService masterClusterConfigurationService;
    private readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    private readonly IMasterHeartbeatService masterHeartbeatService;


    private static readonly int ProbeWindowInSeconds = 10;

    private static string GetProbeWindowKey(DateTime utcNow) =>
        $"{utcNow:yyyyMMddHHmm}{utcNow.Second / ProbeWindowInSeconds}";

    private readonly JobMasterLockKeys lockKeys;

    // Fallbacks control.
    private PollingJobsExecutionRunner? fallBackRunner;
    private BucketModel? fallbackBucket;
    private readonly SemaphoreSlim fallbackCreationLock = new(1, 1);
    private readonly Dictionary<string, DateTime> bucketAssignFirstFailure = new();

    private DateTime LastAssignExecution = DateTime.MinValue;

    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(ProbeWindowInSeconds / 2);
    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(ProbeWindowInSeconds);

    public AssignJobsToBucketsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(
        backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        masterDistributedLockerService =
            backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        masterAgentWorkersService = backgroundAgentWorker.GetClusterAwareService<IMasterAgentWorkersService>();
        masterClusterConfigurationService =
            backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        masterHeartbeatService = backgroundAgentWorker.GetClusterAwareService<IMasterHeartbeatService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (BackgroundAgentWorker.StopRequested)
        {
            return OnTickResult.Skipped(this);
        }

        // Only heartbeat the reserved fallback connection while it's actually backing a bucket —
        // it staying "dead" in the dashboard the rest of the time is a good sign fallback isn't in use.
        if (fallbackBucket is not null)
        {
            masterHeartbeatService.Heartbeat(ResourceHeartbeatType.AgentConnection, fallbackBucket.AgentConnectionId.IdValue);
        }

        var configuration = masterClusterConfigurationService.Get();
        if (configuration?.ClusterMode != ClusterMode.Active)
        {
            return OnTickResult.Skipped(this);
        }


        var transientThreshold = configuration?.TransientThreshold ?? TimeSpan.FromMinutes(5);
        var startTimeUtc = DateTime.UtcNow;
        var durationToLock = JobMasterConstants.DurationToLockRecords;
        var cutOffTime = startTimeUtc.Add(durationToLock).Subtract(JobMasterConstants.ClockSkewPadding);

        // The actual claim lease is deliberately longer (50%) than cutOffTime above -- cutOffTime is
        // when THIS tick voluntarily stops doing more work, but a merely-slow tick that finishes just
        // past it shouldn't already have its claim yanked by another coordinator; the buffer keeps
        // those two thresholds from overlapping.
        var lockExpiryDuration = TimeSpan.FromTicks(durationToLock.Ticks * 3 / 2);
        var transferBatchSize = BackgroundAgentWorker.TransferBatchSize;

        var jobQueryCriteria = new JobQueryCriteria()
        {
            CountLimit = transferBatchSize,
            Status = JobMasterJobStatus.OnMaster,
            NextPlanExecutionAtTo = startTimeUtc.Add(transientThreshold),
            Offset = 0,
            SortBy = new SortByCriteria()
            {
                Property = nameof(JobRawModel.NextPlanExecutionAt),
                Ascending = true,
            },
        };

        // Step 1: Probe -- cheap COUNT + MIN(NextPlanExecutionAt) check that decides whether to run
        // now (imminent path) or defer to the scan-plan interval, and acquires the distributed lock
        // for whichever path applies.
        var probeDiagnosticResult = await ProbeDiagnosticAsync(jobQueryCriteria, transientThreshold);
        if (probeDiagnosticResult.Action == ProbeDiagnosticAction.Skip)
            return OnTickResult.Skipped(this);
        if (probeDiagnosticResult.Action == ProbeDiagnosticAction.Lock)
            return OnTickResult.Locked(TimeSpan.FromSeconds(ProbeWindowInSeconds));
        if (probeDiagnosticResult.Action == ProbeDiagnosticAction.Assign && !probeDiagnosticResult.IsImminent)
            LastAssignExecution = DateTime.UtcNow;

        jobQueryCriteria.CountLimit = probeDiagnosticResult.BatchSize;
        try
        {
            // Step 2: Acquire -- claim OnMaster jobs (PartitionLockId/Expiry set) up to the batch size.
            var acquireSw = Stopwatch.StartNew();
            var jobs = await AcquireJobsAsync(jobQueryCriteria, startTimeUtc.Add(lockExpiryDuration));
            acquireSw.Stop();
            if (jobs.Count <= 0)
            {
                return OnTickResult.Skipped(this);
            }

            // Step 3: In-memory assignment -- decide which bucket each job goes to.
            var assignSw = Stopwatch.StartNew();
            var (assignedJobs, bucketAssignments) = await AssignJobsToBucketsInMemoryAsync(jobs, cutOffTime, durationToLock, ct);
            assignSw.Stop();

            // Step 4: Bulk update on master -- flip assigned jobs from OnMaster to InBucket.
            var bulkUpdateSw = Stopwatch.StartNew();
            var updatedJobs = await BulkUpdateJobsOnMasterAsync(assignedJobs, cutOffTime, durationToLock, ct);
            bulkUpdateSw.Stop();

            // Step 5: Dispatch -- push each updated job to its assigned bucket.
            var dispatchSw = Stopwatch.StartNew();
            await DispatchJobsToBucketsAsync(updatedJobs, bucketAssignments, ct);
            dispatchSw.Stop();

            logger.Debug(
                $"AssignJobsToBucketsRunner: {updatedJobs.Count} jobs assigned. " +
                $"acquireMs={acquireSw.ElapsedMilliseconds} assignMs={assignSw.ElapsedMilliseconds} " +
                $"bulkUpdateMs={bulkUpdateSw.ElapsedMilliseconds} dispatchMs={dispatchSw.ElapsedMilliseconds} " +
                $"JobIds: {string.Join(", ", updatedJobs.Select(x => x.Id))}",
                JobMasterLogCategory.AgentWorker, BackgroundAgentWorker.AgentWorkerId);

            return OnTickResult.Success(this);
        }
        finally
        {
            if (probeDiagnosticResult.LockToken != null)
            {
                masterDistributedLockerService.ReleaseLock(probeDiagnosticResult.LockKey!,
                    probeDiagnosticResult.LockToken);
            }
        }
    }

    // Step 1: Probe -- a cheap COUNT + MIN(NextPlanExecutionAt) check that decides whether to run
    // now (imminent path, time-bucketed distributed lock) or defer to the scan-plan interval
    // (scan-plan path, slot-based distributed lock), acquiring whichever lock applies.
    private async Task<ProbeDiagnosticResult> ProbeDiagnosticAsync(JobQueryCriteria jobQueryCriteria,
        TimeSpan transientThreshold)
    {
        var durationToLock = JobMasterConstants.DurationToLockRecords.Add(TimeSpan.FromMinutes(1));
        var transferBatchSize = BackgroundAgentWorker.TransferBatchSize;
        var probeResult = await this.masterJobsService.ProbeForAcquireAsync(jobQueryCriteria);

        if (probeResult.MinNextPlanExecutionAt.HasValue &&
            probeResult.MinNextPlanExecutionAt <= DateTime.UtcNow.AddSeconds(ProbeWindowInSeconds))
        {
            var windowKey = GetProbeWindowKey(probeResult.MinNextPlanExecutionAt.Value);
            var lockKey = lockKeys.BucketAssignerImminentLock(windowKey);
            var token = this.masterDistributedLockerService.TryLock(lockKey, durationToLock);
            if (!string.IsNullOrEmpty(token))
            {
                return ProbeDiagnosticResult.AssignImminent(transferBatchSize, lockKey, token!);
            }

            return ProbeDiagnosticResult.Locked();
        }

        var workerCount = await BackgroundAgentWorker.WorkerClusterOperations.CountActiveCoordinatorWorkersAsync();
        if (workerCount <= 0)
        {
            workerCount = 1;
        }

        var scanResult = ScanPlanner.ComputeScanPlanHalfWindow(
            probeResult.Count,
            workerCount,
            transferBatchSize,
            transientThreshold,
            lockerLane: 0);

        if ((DateTime.UtcNow - LastAssignExecution) < scanResult.Interval)
        {
            return ProbeDiagnosticResult.Skip();
        }

        var randomKey = JobMasterRandomUtil.GetInt(scanResult.LockerMin, scanResult.LockerMax + 1);
        var lockKey2 = lockKeys.BucketAssignerLock(randomKey);
        var token2 = this.masterDistributedLockerService.TryLock(lockKey2, durationToLock);
        if (!string.IsNullOrEmpty(token2))
        {
            return ProbeDiagnosticResult.Assign(transferBatchSize, lockKey2, token2!);
        }

        return ProbeDiagnosticResult.Locked();
    }

    // Step 2: Acquire -- claims up to jobQueryCriteria.CountLimit OnMaster jobs by setting their
    // PartitionLockId/Expiry, without changing their Status.
    private async Task<IList<JobRawModel>> AcquireJobsAsync(JobQueryCriteria jobQueryCriteria, DateTime lockExpiresAtUtc)
    {
        var jobs = await masterJobsService.AcquireAndFetchAsync(jobQueryCriteria, lockExpiresAtUtc);

        if (jobs.Count > 0)
        {
            logger.Debug(
                $"AssignJobsToBucketsRunner: {jobs.Count} jobs found. JobIds: {string.Join(", ", jobs.Select(x => x.Id))}",
                JobMasterLogCategory.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
        }

        return jobs;
    }

    // Step 3: In-memory assignment -- decides a target bucket for each claimed job (or routes it to
    // fallback assignment) without touching its Status. Jobs are claimed (PartitionLockId/Expiry set)
    // but stay Status=OnMaster until Step 4 flips them to InBucket -- so this whole window, up to that
    // point, is where a slow tick risks its claim lease expiring while another coordinator re-acquires
    // the same rows, causing duplicates. If we're already past cutOffTime, the remaining jobs are left
    // completely untouched (still OnMaster with their original claim) rather than pushing on -- they're
    // safe to expire naturally and get re-acquired cleanly next time, unlike anything that's already
    // been flipped to InBucket.
    private async Task<(List<JobRawModel> AssignedJobs, Dictionary<Guid, BucketModel> BucketAssignments)> AssignJobsToBucketsInMemoryAsync(
        IList<JobRawModel> jobs, DateTime cutOffTime, TimeSpan durationToLock, CancellationToken ct)
    {
        var assignedJobs = new List<JobRawModel>(jobs.Count);
        var bucketAssignments = new Dictionary<Guid, BucketModel>();
        var jobIdsNeedingLockRelease = new List<Guid>();
        var assignmentTimedOut = false;

        foreach (var job in jobs)
        {
            if (DateTime.UtcNow >= cutOffTime)
            {
                if (!assignmentTimedOut)
                {
                    assignmentTimedOut = true;
                    logger.Warn(
                        $"AssignJobsToBucketsRunner: bucket assignment exceeded the {durationToLock} claim lock duration. " +
                        "Stopping early -- remaining jobs are left untouched (still claimed OnMaster) and will be re-acquired safely once the lock lapses.",
                        JobMasterLogCategory.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
                }

                continue;
            }

            var (result, bucket) = await HandleJobBucketAssignmentAsync(job, ct, jobIdsNeedingLockRelease);
            if (result == HandleJobBucketAssignmentResult.Canceled)
            {
                break;
            }

            if (result == HandleJobBucketAssignmentResult.Success)
            {
                assignedJobs.Add(job);
            }

            if (bucket != null)
            {
                bucketAssignments[job.Id] = bucket;
            }
        }

        // Batched instead of one release call per job -- this path can fire for many jobs in a
        // single tick when buckets are unavailable/at capacity, and each release now also
        // contends for the same acquire throttler as AcquireAndFetchAsync/BulkUpdateAsync.
        var masterMaxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(BackgroundAgentWorker.ClusterConnConfig.ClusterId);
        foreach (var partition in jobIdsNeedingLockRelease.Partition(masterMaxBatchSize))
        {
            await masterJobsService.BulkUpdateAsync(BulkJobUpdateRequest.ReleasePartitionLock(partition.ToList()), masterJobsService.AcquireThrottler);
        }

        return (assignedJobs, bucketAssignments);
    }

    // Step 4: Bulk update on master -- flips each assigned job's Status from OnMaster to InBucket,
    // partitioned to respect the bulk-operation batch size.
    private async Task<List<JobRawModel>> BulkUpdateJobsOnMasterAsync(
        List<JobRawModel> assignedJobs, DateTime cutOffTime, TimeSpan durationToLock, CancellationToken ct)
    {
        var updatedJobs = new List<JobRawModel>();
        var masterMaxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(BackgroundAgentWorker.ClusterConnConfig.ClusterId);
        foreach (var partition in assignedJobs.Partition(masterMaxBatchSize))
        {
            if (DateTime.UtcNow >= cutOffTime)
            {
                logger.Warn(
                    $"AssignJobsToBucketsRunner: bulk-update phase exceeded the {durationToLock} claim lock duration. " +
                    "Stopping early -- remaining jobs are left untouched (still claimed OnMaster) and will be re-acquired safely once the lock lapses.",
                    JobMasterLogCategory.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
                break;
            }

            // Passing AcquireThrottler routes this through the same 1-at-a-time, per-cluster
            // (all coordinators) throttler as AcquireAndFetchAsync -- otherwise this used the
            // general, much-higher-capacity throttler, letting two coordinators' bulk updates
            // and acquires run fully concurrently against each other.
            var updated = await masterJobsService.BulkUpdateAsync(partition.ToList(), masterJobsService.AcquireThrottler);
            updatedJobs.AddRange(updated);

            // Small per-partition jitter so this coordinator's back-to-back UPDATEs don't stay
            // phase-locked with another coordinator's own partition loop for the whole bulk-update
            // phase -- cheap since each partition is one multi-row UPDATE, not many single-row ones.
            await Task.Delay(TimeSpan.FromMilliseconds(JobMasterRandomUtil.GetInt(0, 11)), ct);
        }

        return updatedJobs;
    }

    // Step 5: Dispatch -- pushes each updated job to its assigned bucket. Jobs that fail to dispatch
    // are moved back to OnMaster (HeldOnMaster) so they're re-acquired cleanly next tick instead of
    // staying stuck InBucket with a stale bucket/lock assignment.
    private async Task DispatchJobsToBucketsAsync(
        List<JobRawModel> updatedJobs, Dictionary<Guid, BucketModel> bucketAssignments, CancellationToken ct)
    {
        // Grouping by bucket and partitioning happen here rather than inside the dispatcher so a
        // failed partition only holds back that partition's jobs, not the whole tick's batch.
        var jobsByBucket = new Dictionary<string, (BucketModel Bucket, List<JobRawModel> Jobs)>();
        foreach (var job in updatedJobs)
        {
            if (!bucketAssignments.TryGetValue(job.Id, out var bucket))
            {
                continue;
            }

            if (!jobsByBucket.TryGetValue(bucket.Id, out var entry))
            {
                entry = (bucket, new List<JobRawModel>());
                jobsByBucket[bucket.Id] = entry;
            }

            entry.Jobs.Add(job);
        }

        // Two-level parallelism: distinct agent connections are separate DBs/brokers, so they
        // fan out first (capped so a huge cluster doesn't open too many connections at once);
        // buckets within one agent connection fan out too, bounded lower since they share that
        // connection's own OperationThrottler.
        var bucketGroupsByAgentConnection = jobsByBucket.Values
            .GroupBy(entry => entry.Bucket.AgentConnectionId.IdValue)
            .ToList();

        var agentConnectionParallelOptions = new ParallelOptions
        {
            CancellationToken = ct,
            MaxDegreeOfParallelism = JobMasterConstants.MaxParallelAgentConnectionDispatch,
        };

        await JobMasterParallelUtil.ForEachAsync(bucketGroupsByAgentConnection, agentConnectionParallelOptions,
            async (agentConnectionGroup, _) =>
            {
                var bucketParallelOptions = new ParallelOptions
                {
                    CancellationToken = ct,
                    MaxDegreeOfParallelism = JobMasterConstants.MaxParallelBucketDispatchPerAgentConnection,
                };

                await JobMasterParallelUtil.ForEachAsync(agentConnectionGroup, bucketParallelOptions,
                    async (entry, __) =>
                    {
                        var (bucket, bucketJobs) = entry;
                        var agentMaxBatchSize = OperationThrottlerSettingsFactory.GetAgentMaxBatchSize(bucket.AgentConnectionId.IdValue);
                        foreach (var partition in bucketJobs.Partition(agentMaxBatchSize))
                        {
                            try
                            {
                                await BackgroundAgentWorker.WorkerClusterOperations.BulkDispatchJobsToBucketAsync(
                                    BackgroundAgentWorker, bucket, partition.ToList());
                            }
                            catch (Exception ex)
                            {
                                // Only log error here. the operation is responsible for holding the partition on back master
                                logger.Error(
                                    $"Failed to dispatch partition of {partition.Count} jobs to bucket {bucket.Id}. Continuing with remaining partitions.",
                                    JobMasterLogCategory.AgentWorker, BackgroundAgentWorker.AgentWorkerId, exception: ex);
                            }
                        }
                    });
            });
    }

    public override async Task OnStopAsync()
    {
        await base.OnStopAsync();
        await MarkFallbackBucketAsReadyToDeleteAsync();
    }

    public override async Task OnTerminateFailureAsync(Exception ex)
    {
        await base.OnTerminateFailureAsync(ex);
        await MarkFallbackBucketAsReadyToDeleteAsync();
    }

    private async Task MarkFallbackBucketAsReadyToDeleteAsync()
    {
        if (!string.IsNullOrEmpty(this.fallbackBucket?.Id))
        {
            // There is no draining for the fallback bucket, so it can be safely marked as ready to delete.
            // In the worst case, jobs will be reassigned when they reach their deadline.
            await this.BackgroundAgentWorker.WorkerClusterOperations.MarkBucketAsReadyToDeleteAsync(fallbackBucket!.Id);
        }
    }


    private async Task<(HandleJobBucketAssignmentResult, BucketModel?)> HandleJobBucketAssignmentAsync(
        JobRawModel job,
        CancellationToken ct,
        List<Guid> jobIdsNeedingLockRelease)
    {
        if (job.Status != JobMasterJobStatus.OnMaster)
        {
            logger.Error($"Job {job.Id} is not held on master. This is not allowed.", JobMasterLogCategory.Job,
                job.Id);
            jobIdsNeedingLockRelease.Add(job.Id);
            return (HandleJobBucketAssignmentResult.Failed, null);
        }

        if (ct.IsCancellationRequested)
        {
            return (HandleJobBucketAssignmentResult.Canceled, null);
        }

        var bucket = await masterBucketsService.SelectBucketAsync(
            JobMasterConstants.BucketFastAllowDiscrepancy,
            job.Priority,
            job.WorkerLane);

        if (bucket is null)
        {
            // No standard bucket available. HandleJobFallbackAssignmentAsync either assigns the
            // fallback bucket in-memory (same as a normal assignment, below) so it flows through
            // the same bulk-update/dispatch steps as any other job, or returns null to delay the
            // job and queue its lock for release instead.
            bucket = await HandleJobFallbackAssignmentAsync(job, jobIdsNeedingLockRelease);
            return bucket != null
                ? (HandleJobBucketAssignmentResult.Success, bucket)
                : (HandleJobBucketAssignmentResult.Failed, null);
        }

        bucketAssignFirstFailure.Remove(BucketFailureKey(job));
        job.AssignToBucket(bucket);

        return (HandleJobBucketAssignmentResult.Success, bucket);
    }

    // Returns the fallback bucket once NoBucketFallbackThreshold has elapsed with no standard
    // bucket found, or null while still within the threshold (the job is delayed and its lock
    // queued for release instead).
    private async Task<BucketModel?> HandleJobFallbackAssignmentAsync(JobRawModel job, List<Guid> jobIdsNeedingLockRelease)
    {
        var bucketKey = BucketFailureKey(job);
        if (!bucketAssignFirstFailure.TryGetValue(bucketKey, out var firstFailure))
        {
            firstFailure = DateTime.UtcNow;
            bucketAssignFirstFailure[bucketKey] = firstFailure;
        }

        var elapsed = DateTime.UtcNow - firstFailure;

        if (elapsed >= JobMasterConstants.NoBucketFallbackThreshold)
        {
            logger.Warn(
                $"No available bucket found for job {job.Id} (Lane={job.WorkerLane}, Priority={job.Priority}). Using fallback bucket.",
                JobMasterLogCategory.Job, job.Id);

            var bucket = await EnsureFallbackBucketAsync();
            job.AdvanceNextExecutionPlan(JobMasterConstants.NoBucketFallbackThreshold);
            job.AssignToBucket(bucket);
            return bucket;
        }
        else
        {
            job.DelayNextExecutionPlan(JobMasterConstants.NoBucketFallbackThreshold.Add(TimeSpan.FromMinutes(1)));
            await masterJobsService.UpdateAsync(job);
            logger.Warn(
                $"No available bucket found for job {job.Id} (Lane={job.WorkerLane}, Priority={job.Priority}). Retrying in {JobMasterConstants.NoBucketFallbackThreshold.TotalMinutes:F1} minutes.",
                JobMasterLogCategory.Job, job.Id);
        }

        jobIdsNeedingLockRelease.Add(job.Id);
        return null;
    }
    
    private async Task<BucketModel> EnsureFallbackBucketAsync()
    {
        if (fallbackBucket is not null)
        {
            return fallbackBucket;
        }

        await fallbackCreationLock.WaitAsync();
        try
        {
            if (fallbackBucket is not null)
            {
                return fallbackBucket;
            }

            this.logger.Critical(
                $"Fallback bucket activated: no standard bucket could be assigned for over {JobMasterConstants.NoBucketFallbackThreshold.TotalMinutes} minutes. " +
                "This usually means no bucket matches the required lane/priority, or all agents are offline. " +
                "A temporary bucket backed by the master database will be used to prevent job starvation. Review your worker lanes, priority configuration, and agent health.",
                JobMasterLogCategory.AgentWorker,
                BackgroundAgentWorker.AgentWorkerId);

            var fallbackConnConfig = BackgroundAgentWorker.ClusterConnConfig.GetAgentConnectionConfig(JobMasterConstants.MasterFallbackAgentConnName);
            var fallbackConnectionId = new AgentConnectionId(fallbackConnConfig.Id);
            var fallbackPriority = ResolveFallbackPriority();

            var bucket = await this.masterBucketsService.CreateAsync(
                fallbackConnectionId,
                BackgroundAgentWorker.AgentWorkerId,
                fallbackPriority,
                BucketType.Fallback);
            this.fallbackBucket = bucket;

            // Register the fallback bucket so GetOrCreateEngine can validate it on the runner's first tick.
            BackgroundAgentWorker.RegisterRuntimeBucket(bucket);

            var source = new StandardBucketJobsOnboardingSource(agentJobsDispatcherService, fallbackConnectionId, bucket.Id);
            fallBackRunner = PollingJobsExecutionRunner.Create(
                this.BackgroundAgentWorker,
                source);
            fallBackRunner.DefineBucketId(bucket.Id, fallbackPriority);
            await fallBackRunner.StartAsync();

            return fallbackBucket;
        }
        finally
        {
            fallbackCreationLock.Release();
        }
    }

    // Preference order for the fallback bucket's priority: prefer Critical (fastest drain), falling back to
    // the next-highest priority the cluster hasn't disabled. Medium can never be disabled (see
    // ClusterConfigBuilder.DisablePriority), so this is always guaranteed to resolve.
    private static readonly JobMasterPriority[] FallbackPriorityPreference =
    {
        JobMasterPriority.Critical,
        JobMasterPriority.High,
        JobMasterPriority.Medium,
    };

    private JobMasterPriority ResolveFallbackPriority()
    {
        foreach (var priority in FallbackPriorityPreference)
        {
            if (!BackgroundAgentWorker.ClusterConnConfig.IsPriorityDisabled(priority))
            {
                return priority;
            }
        }

        return JobMasterPriority.Medium;
    }

    private string BucketFailureKey(JobRawModel job) =>
        $"{BackgroundAgentWorker.ClusterConnConfig.ClusterId}_{job.WorkerLane}_{job.Priority}";

    private enum HandleJobBucketAssignmentResult
    {
        Success,
        Failed,
        Canceled
    }
}
