using System.Collections.Concurrent;
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

namespace JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

internal class AssignJobsToBucketsRunner : JobMasterRunner
{
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IMasterJobsService masterJobsService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly IMasterAgentWorkersService masterAgentWorkersService;
    private readonly IMasterClusterConfigurationService masterClusterConfigurationService;


    private ScanPlanResult? lastScanPlanResult;

    private readonly JobMasterLockKeys lockKeys;

    private ManualJobsExecutionRunner? fallBackRunner;
    private BucketModel? fallbackBucket;
    private readonly SemaphoreSlim fallbackCreationLock = new(1, 1);
    private static readonly ConcurrentDictionary<string, DateTime> BucketAssignFirstFailure = new();

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(5);

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

        var scanPlanResult = await ComputeScanPlanAsync(jobQueryCriteria, transferBatchSize, transientThreshold);
        jobQueryCriteria.CountLimit = scanPlanResult.BatchSize;

        var bucketAssignerSlot = JobMasterRandomUtil.GetInt(scanPlanResult.LockerMin, scanPlanResult.LockerMax + 1);
        var lockToken = masterDistributedLockerService.TryLock(lockKeys.BucketAssignerLock(bucketAssignerSlot ), durationToLock.Add(TimeSpan.FromMinutes(1)));
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromSeconds(10));
        }

        var jobs = await masterJobsService.AcquireAndFetchAsync(jobQueryCriteria, startTimeUtc.Add(durationToLock));
        if (jobs.Count <= 0)
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.BucketAssignerLock(bucketAssignerSlot ), lockToken);
            return OnTickResult.Skipped(scanPlanResult.Interval);
        }

        logger.Debug(
            $"AssignJobsToBucketsRunner: {jobs.Count} jobs found. JobIds: {string.Join(", ", jobs.Select(x => x.Id))}",
            JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);

        var jobIdByBucketModel = new Dictionary<Guid, BucketModel>();

        // Assign buckets first because the cache, assign buckets in separate foreach loop.
        foreach (var job in new List<JobRawModel>(jobs))
        {
           var result = await HandleJobBucketAssignmentAsync(job, jobIdByBucketModel, ct);
           if (result == HandleJobBucketAssignmentResult.Canceled)
           {
               break;
           }
           
           if (result == HandleJobBucketAssignmentResult.Failed || result == HandleJobBucketAssignmentResult.FallbackAssignment)
           {
               jobs.Remove(job);
           }
        }

        var timeRemaining = cutOffTime - DateTime.UtcNow;
        using var batchTimeoutCts =
            new CancellationTokenSource(timeRemaining > TimeSpan.FromSeconds(5) ? timeRemaining : TimeSpan.FromSeconds(5));
        var parallelOptions = new ParallelOptions()
        {
            CancellationToken = batchTimeoutCts.Token,
            MaxDegreeOfParallelism = 10,
        };
        await JobMasterParallelUtil.ForEachAsync(
            jobs, 
            parallelOptions, 
            async (job, _) => {
                if (cutOffTime <= DateTime.UtcNow)
                {
                    logger.Warn($"Take too long to assign jobs to buckets.", JobMasterLogSubjectType.AgentWorker,
                        BackgroundAgentWorker.AgentWorkerId);
                    return;
                }

                await DispatchJobToBucketAsync(jobIdByBucketModel, job);
            });

        masterDistributedLockerService.ReleaseLock(lockKeys.BucketAssignerLock(bucketAssignerSlot ), lockToken);

        if (BackgroundAgentWorker.IsOnWarmUpTime() && WarmUpInterval < scanPlanResult.Interval)
        {
            return OnTickResult.Success(WarmUpInterval);
        }

        return OnTickResult.Success(scanPlanResult.Interval);
    }
    
    public override async Task OnStopAsync()
    {
        await base.OnStopAsync();
        await MarkFallbackBucketAsReadyToDeleteAsync();
    }

    public override async Task OnErrorAsync(Exception ex, CancellationToken ct)
    {
        await base.OnErrorAsync(ex, ct);
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

        logger.Debug($"Assigning job {job.Id} to bucket {bucket.Id}", JobMasterLogSubjectType.Job, job.Id);

        try
        {
            await BackgroundAgentWorker.WorkerClusterOperations.AssignJobToBucketFromHeldOnMasterOrSavePendingAsync(
                this.BackgroundAgentWorker, job, bucket);
        }
        catch (Exception e)
        {
            logger.Error($"Failed to assign job to bucket. JobId={job.Id}", JobMasterLogSubjectType.Job, job.Id,
                exception: e);
        }
    }

    private async Task<ScanPlanResult> ComputeScanPlanAsync(JobQueryCriteria jobQueryCriteria, int transferBatchSize,
        TimeSpan transientThreshold)
    {
        if (lastScanPlanResult == null || lastScanPlanResult.ShouldCalculateAgain())
        {
            var countJobs = masterJobsService.Count(jobQueryCriteria);
            var workerCount = await BackgroundAgentWorker.WorkerClusterOperations.CountActiveCoordinatorWorkersAsync();
            if (workerCount <= 0)
            {
                workerCount = 1;
            }

            lastScanPlanResult = ScanPlanner.ComputeScanPlanHalfWindow(
                countJobs,
                workerCount,
                transferBatchSize,
                transientThreshold,
                lockerLane: 0);
        }
        
        return lastScanPlanResult;
    }
    
    private async Task<HandleJobBucketAssignmentResult> HandleJobBucketAssignmentAsync(
        JobRawModel job, 
        Dictionary<Guid, BucketModel> jobIdByBucketModel, 
        CancellationToken ct)
    {
        if (job.Status != JobMasterJobStatus.OnMaster)
        {
            logger.Error($"Job {job.Id} is not held on master. This is not allowed.", JobMasterLogSubjectType.Job,
                job.Id);
            masterJobsService.ReleasePartitionLock(job.Id);
            return HandleJobBucketAssignmentResult.Failed;
        }

        if (ct.IsCancellationRequested)
        {
            return HandleJobBucketAssignmentResult.Canceled;
        }

        var bucket = await GetBucketAvailableForJobAsync(job);
        if (bucket is null)
        { 
            await HandleJobFallbackAssignmentAsync(job);
            return HandleJobBucketAssignmentResult.FallbackAssignment;
        }

        BucketAssignFirstFailure.TryRemove(BucketFailureKey(job), out _);
        
        if (!jobIdByBucketModel.TryGetValue(job.Id, out _))
        {
            jobIdByBucketModel.Add(job.Id, bucket!);
        }

        return HandleJobBucketAssignmentResult.Success;
    }

    private async Task HandleJobFallbackAssignmentAsync(JobRawModel job)
    {
        var bucketKey = BucketFailureKey(job);
        if (!BucketAssignFirstFailure.TryGetValue(bucketKey, out var firstFailure))
        {
            firstFailure = DateTime.UtcNow;
            BucketAssignFirstFailure.TryAdd(bucketKey, firstFailure);
        }

        var elapsed = DateTime.UtcNow - firstFailure;

        if (elapsed >= JobMasterConstants.NoBucketFallbackThreshold)
        {
            logger.Warn(
                $"No available bucket found for job {job.Id} (Lane={job.WorkerLane}, Priority={job.Priority}). ",
                JobMasterLogSubjectType.Job, job.Id);

            var fallbackOnboardingSource = await EnsureFallbackOnboardingSourceAsync();
            job.AdvanceNextExecutionPlan(JobMasterConstants.NoBucketFallbackThreshold);
            job.AssignToBucket(this.fallbackBucket!);
            var pushed = await fallbackOnboardingSource.PushAsync(job);
            if (pushed)
            {
                await masterJobsService.UpsertAsync(job);
                return;
            }

            logger.Critical(
                $"Fallback bucket is at capacity. Job {job.Id} (Lane={job.WorkerLane}, Priority={job.Priority}) could not be queued. " +
                "This indicates the fallback engine is overloaded. Job will be delayed and retried.",
                JobMasterLogSubjectType.Job, job.Id);
            job.MarkAsHeldOnMaster();
            job.DelayNextExecutionPlan(JobMasterConstants.NoBucketFallbackThreshold.Add(TimeSpan.FromMinutes(1)));

            await masterJobsService.UpsertAsync(job);
        }
        else
        {
            job.DelayNextExecutionPlan(JobMasterConstants.NoBucketFallbackThreshold.Add(TimeSpan.FromMinutes(1)));
            await masterJobsService.UpsertAsync(job);
            logger.Warn(
                $"No available bucket found for job {job.Id} (Lane={job.WorkerLane}, Priority={job.Priority}). Retrying in {JobMasterConstants.NoBucketFallbackThreshold.TotalMinutes:F1} mins",
                JobMasterLogSubjectType.Job, job.Id);
        }

        masterJobsService.ReleasePartitionLock(job.Id);
    }

    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(30);

    private async Task<BucketModel?> GetBucketAvailableForJobAsync(JobRawModel job)
    {
        return await masterBucketsService.SelectBucketAsync(
            JobMasterConstants.BucketFastAllowDiscrepancy,
            job.Priority,
            job.WorkerLane);
    }

    private async Task<IJobsOnboardingSource> EnsureFallbackOnboardingSourceAsync()
    {
        if (fallBackRunner is not null)
        {
            return fallBackRunner.JobsOnboardingSource;
        }

        await fallbackCreationLock.WaitAsync();
        try
        {
            if (fallBackRunner is not null)
            {
                return fallBackRunner.JobsOnboardingSource;
            }

            this.logger.Critical(
                $"Fallback bucket activated: no standard bucket could be assigned for over {JobMasterConstants.NoBucketFallbackThreshold.TotalMinutes} minutes. " +
                "This usually means no bucket matches the required lane/priority, or all agents are offline. " +
                "A temporary local bucket will be used to prevent job starvation. Review your worker lanes, priority configuration, and agent health.",
                JobMasterLogSubjectType.AgentWorker,
                BackgroundAgentWorker.AgentWorkerId);

            var bucket = await this.masterBucketsService.CreateAsync(
                BackgroundAgentWorker.AgentConnectionId,
                BackgroundAgentWorker.AgentWorkerId,
                JobMasterPriority.Critical,
                BucketType.Fallback);
            this.fallbackBucket = bucket;

            var jobExecutionEngine = new JobsExecutionEngine(this.BackgroundAgentWorker, bucket.Id, JobMasterPriority.Critical);
            fallBackRunner = ManualJobsExecutionRunner.Create(this.BackgroundAgentWorker, jobExecutionEngine);
            fallBackRunner.DefineBucketId(bucket.Id, BucketType.Fallback, JobMasterPriority.Critical);
            await fallBackRunner.StartAsync();

            return fallBackRunner.JobsOnboardingSource;
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
}