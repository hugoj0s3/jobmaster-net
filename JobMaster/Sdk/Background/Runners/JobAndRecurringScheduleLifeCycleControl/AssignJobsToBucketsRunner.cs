using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
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


    private static readonly int ProbeWindowInSeconds = 10;

    private static string GetProbeWindowKey(DateTime utcNow) =>
        $"{utcNow:yyyyMMddHHmm}{utcNow.Second / ProbeWindowInSeconds}";

    private readonly JobMasterLockKeys lockKeys;

    private ManualJobsExecutionRunner? fallBackRunner;
    private FallbackBucketJobsOnboardingSource? fallbackOnboardingSource;
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
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (BackgroundAgentWorker.StopRequested)
        {
            return OnTickResult.Skipped(this);
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

        var probeDiagnosticResult = await ProbeDiagnosticAsync(jobQueryCriteria, transientThreshold);
        if (probeDiagnosticResult.Action == ProbeDiagnosticAction.Skip)
            return OnTickResult.Skipped(this);
        if (probeDiagnosticResult.Action == ProbeDiagnosticAction.Lock)
            return OnTickResult.Locked(TimeSpan.FromSeconds(ProbeWindowInSeconds));
        if (probeDiagnosticResult.Action == ProbeDiagnosticAction.Assign && !probeDiagnosticResult.IsImminent)
            LastAssignExecution = DateTime.UtcNow;

        jobQueryCriteria.CountLimit = probeDiagnosticResult.BatchSize;

        var jobs = await masterJobsService.AcquireAndFetchAsync(jobQueryCriteria, startTimeUtc.Add(durationToLock));
        if (jobs.Count <= 0)
        {
            masterDistributedLockerService.ReleaseLock(probeDiagnosticResult.LockKey!, probeDiagnosticResult.LockToken);
            return OnTickResult.Skipped(this);
        }

        logger.Debug(
            $"AssignJobsToBucketsRunner: {jobs.Count} jobs found. JobIds: {string.Join(", ", jobs.Select(x => x.Id))}",
            JobMasterLogCategory.AgentWorker, BackgroundAgentWorker.AgentWorkerId);

        var bucketAssignments = new Dictionary<Guid, BucketModel>();
        foreach (var job in new List<JobRawModel>(jobs))
        {
            var (result, bucket) = await HandleJobBucketAssignmentAsync(job, ct);
            if (result == HandleJobBucketAssignmentResult.Canceled)
            {
                break;
            }

            if (result == HandleJobBucketAssignmentResult.Failed ||
                result == HandleJobBucketAssignmentResult.FallbackAssignment)
            {
                jobs.Remove(job);
            }
            
            if (bucket != null)
            {
                bucketAssignments[job.Id] = bucket;
            }
        }

        var updatedJobs = new List<JobRawModel>();
        foreach (var partition in jobs.Partition(JobMasterConstants.MaxBatchSizeForBulkOperation))
        {
            var updated = await masterJobsService.BulkUpdateAsync(partition.ToList());
            updatedJobs.AddRange(updated);
        }

        var timeRemaining = cutOffTime - DateTime.UtcNow;
        using var batchTimeoutCts =
            new CancellationTokenSource(timeRemaining > TimeSpan.FromSeconds(5) ? timeRemaining : TimeSpan.FromSeconds(5));
        var parallelOptions = new ParallelOptions()
        {
            CancellationToken = batchTimeoutCts.Token,
            MaxDegreeOfParallelism = 5,
        };
        
        await JobMasterParallelUtil.ForEachAsync(
            updatedJobs, 
            parallelOptions, 
            async (job, _) => {
                if (cutOffTime <= DateTime.UtcNow)
                {
                    logger.Warn($"Assigning jobs to buckets is taking too long. Stopping early.", JobMasterLogCategory.AgentWorker,
                        BackgroundAgentWorker.AgentWorkerId);
                    return;
                }

                await DispatchJobToBucketAsync(bucketAssignments, job);
            });

        logger.Debug(
            $"AssignJobsToBucketsRunner: {updatedJobs.Count} jobs assigned. JobIds: {string.Join(", ", updatedJobs.Select(x => x.Id))}",
            JobMasterLogCategory.AgentWorker, BackgroundAgentWorker.AgentWorkerId);

        masterDistributedLockerService.ReleaseLock(probeDiagnosticResult.LockKey!, probeDiagnosticResult.LockToken);

        return OnTickResult.Success(this);
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


    private async Task DispatchJobToBucketAsync(IReadOnlyDictionary<Guid, BucketModel> jobIdByBucketModel, JobRawModel job)
    {
        if (!jobIdByBucketModel.TryGetValue(job.Id, out var bucket))
        {
            return;
        }

        logger.Debug($"Assigning job {job.Id} to bucket {bucket.Id}", JobMasterLogCategory.Job, job.Id);

        try
        {
            await BackgroundAgentWorker.WorkerClusterOperations.DispatchJobToBucketAsync(this.BackgroundAgentWorker, job, bucket);
        }
        catch (Exception e)
        {
            logger.Error($"Failed to assign job to bucket. JobId={job.Id}", JobMasterLogCategory.Job, job.Id,
                exception: e);
        }
    }

    private async Task<(HandleJobBucketAssignmentResult, BucketModel?)> HandleJobBucketAssignmentAsync(
        JobRawModel job,
        CancellationToken ct)
    {
        if (job.Status != JobMasterJobStatus.OnMaster)
        {
            logger.Error($"Job {job.Id} is not held on master. This is not allowed.", JobMasterLogCategory.Job,
                job.Id);
            masterJobsService.ReleasePartitionLock(job.Id);
            return (HandleJobBucketAssignmentResult.Failed, null);
        }

        if (ct.IsCancellationRequested)
        {
            return (HandleJobBucketAssignmentResult.Canceled, null);
        }

        var bucket = await GetBucketAvailableForJobAsync(job);
        if (bucket is null)
        {
            await HandleJobFallbackAssignmentAsync(job);
            return (HandleJobBucketAssignmentResult.FallbackAssignment, null);
        }

        bucketAssignFirstFailure.Remove(BucketFailureKey(job));
        job.AssignToBucket(bucket);

        return (HandleJobBucketAssignmentResult.Success, bucket);
    }

    private async Task HandleJobFallbackAssignmentAsync(JobRawModel job)
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

            var fallbackSource = await EnsureFallbackOnboardingSourceAsync();
            job.AdvanceNextExecutionPlan(JobMasterConstants.NoBucketFallbackThreshold);
            job.AssignToBucket(this.fallbackBucket!);
            await masterJobsService.UpdateAsync(job);
            await fallbackSource.PushAsync(job);
            return;
        }
        else
        {
            job.DelayNextExecutionPlan(JobMasterConstants.NoBucketFallbackThreshold.Add(TimeSpan.FromMinutes(1)));
            await masterJobsService.UpdateAsync(job);
            logger.Warn(
                $"No available bucket found for job {job.Id} (Lane={job.WorkerLane}, Priority={job.Priority}). Retrying in {JobMasterConstants.NoBucketFallbackThreshold.TotalMinutes:F1} minutes.",
                JobMasterLogCategory.Job, job.Id);
        }

        masterJobsService.ReleasePartitionLock(job.Id);
    }

    private async Task<BucketModel?> GetBucketAvailableForJobAsync(JobRawModel job)
    {
        return await masterBucketsService.SelectBucketAsync(
            JobMasterConstants.BucketFastAllowDiscrepancy,
            job.Priority,
            job.WorkerLane);
    }

    private async Task<FallbackBucketJobsOnboardingSource> EnsureFallbackOnboardingSourceAsync()
    {
        if (fallbackOnboardingSource is not null)
        {
            return fallbackOnboardingSource;
        }

        await fallbackCreationLock.WaitAsync();
        try
        {
            if (fallbackOnboardingSource is not null)
            {
                return fallbackOnboardingSource;
            }

            this.logger.Critical(
                $"Fallback bucket activated: no standard bucket could be assigned for over {JobMasterConstants.NoBucketFallbackThreshold.TotalMinutes} minutes. " +
                "This usually means no bucket matches the required lane/priority, or all agents are offline. " +
                "A temporary local bucket will be used to prevent job starvation. Review your worker lanes, priority configuration, and agent health.",
                JobMasterLogCategory.AgentWorker,
                BackgroundAgentWorker.AgentWorkerId);

            var bucket = await this.masterBucketsService.CreateAsync(
                BackgroundAgentWorker.AgentConnectionId,
                BackgroundAgentWorker.AgentWorkerId,
                JobMasterPriority.Critical,
                BucketType.Fallback);
            this.fallbackBucket = bucket;

            // Register the fallback bucket so GetOrCreateEngine can validate it on the runner's first tick.
            BackgroundAgentWorker.RegisterRuntimeBucket(bucket);

            var source = new FallbackBucketJobsOnboardingSource();
            fallBackRunner = ManualJobsExecutionRunner.Create(
                this.BackgroundAgentWorker,
                source);
            fallBackRunner.DefineBucketId(bucket.Id, JobMasterPriority.Critical);
            fallbackOnboardingSource = source;
            await fallBackRunner.StartAsync();

            return fallbackOnboardingSource;
        }
        finally
        {
            fallbackCreationLock.Release();
        }
    }
    
    private string BucketFailureKey(JobRawModel job) =>
        $"{BackgroundAgentWorker.ClusterConnConfig.ClusterId}_{job.WorkerLane}_{job.Priority}";
    
    private enum HandleJobBucketAssignmentResult 
    {
        Success,
        FallbackAssignment,
        Failed,
        Canceled
    }

    private async Task<ProbeDiagnosticResult> ProbeDiagnosticAsync(JobQueryCriteria jobQueryCriteria, TimeSpan transientThreshold)
    {
        var durationToLock = JobMasterConstants.DurationToLockRecords.Add(TimeSpan.FromMinutes(1));
        var transferBatchSize = BackgroundAgentWorker.TransferBatchSize;
        var probeResult = await this.masterJobsService.ProbeForBucketAssignmentAsync(jobQueryCriteria);

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
}
