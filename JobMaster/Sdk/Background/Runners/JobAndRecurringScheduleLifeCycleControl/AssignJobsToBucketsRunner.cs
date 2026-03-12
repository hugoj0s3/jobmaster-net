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
    
    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(5);
    
    public AssignJobsToBucketsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        masterAgentWorkersService = backgroundAgentWorker.GetClusterAwareService<IMasterAgentWorkersService>();
        masterClusterConfigurationService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
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

        var utcNow = DateTime.UtcNow;
        var durationToLock = JobMasterConstants.DurationToLockRecords;
        var cutOffTime = utcNow.Add(durationToLock).AddSeconds(-30);
        var jobQueryCriteria = new JobQueryCriteria()
        {
            CountLimit = BackgroundAgentWorker.TransferBatchSize,
            Status = JobMasterJobStatus.OnMaster,
            NextPlanExecutionAtTo = utcNow.Add(transientThreshold),
            IsLocked = false,
            Offset = 0,
            SortBy = new SortByCriteria()
            {
                Property = nameof(JobRawModel.NextPlanExecutionAt),
                Ascending = true,
            },
        };
        
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
                BackgroundAgentWorker.TransferBatchSize,
                transientThreshold,
                lockerLane:0);
        }
        jobQueryCriteria.CountLimit = lastScanPlanResult.BatchSize;
        
        var lockId = JobMasterRandomUtil.GetInt(lastScanPlanResult.LockerMin, lastScanPlanResult.LockerMax + 1);
        
        var lockToken = masterDistributedLockerService.TryLock(lockKeys.BucketAssignerLock(lockId), durationToLock.Add(TimeSpan.FromMinutes(1)));
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromSeconds(10));
        }

        var jobs = await masterJobsService.AcquireAndFetchAsync(jobQueryCriteria, lockId, utcNow.Add(durationToLock));
        if (jobs.Count <= 0)
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.BucketAssignerLock(lockId), lockToken);
            return OnTickResult.Skipped(lastScanPlanResult.Interval);
        }

        logger.Debug($"AssignJobsToBucketsRunner: {jobs.Count} jobs found. JobIds: {string.Join(", ", jobs.Select(x => x.Id))}", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
        
        var jobIdByBucketModel = new Dictionary<Guid, BucketModel>(); 
        
        // Assign buckets first because the cache, assign buckets in separate foreach loop.
        foreach (var job in jobs)
        {
            if (job.Status != JobMasterJobStatus.OnMaster)
            {
                logger.Error($"Job {job.Id} is not held on master. This is not allowed.", JobMasterLogSubjectType.Job, job.Id);
            }
            
            if (ct.IsCancellationRequested)
            {
                break;
            }
            
            var bucket = await GetBucketAvailableForJobAsync(job);
            if (bucket is null)
            {
                if (job.ScheduledAt <= DateTime.UtcNow.AddMinutes(-10))
                {
                    var isRetrying = job.TryRetry();
                    await masterJobsService.UpsertAsync(job);
                    if (isRetrying)
                    {
                        logger.Error($"No available bucket found for job {job.Id}. " +
                                        $"Retrying {job.NumberOfFailures}/{job.MaxNumberOfRetries}", JobMasterLogSubjectType.Job, job.Id);
                    }
                    else
                    {
                        logger.Critical($"No available bucket found for job {job.Id}. Marked as failed",
                            JobMasterLogSubjectType.Job, job.Id);
                    }
                }
                else
                {
                    job.DelayNextExecutionPlan(TimeSpan.FromMinutes(2.5));
                    await masterJobsService.UpsertAsync(job);
                    logger.Warn($"No available bucket found for job {job.Id}. Retrying in 2.5 mins", JobMasterLogSubjectType.Job, job.Id);
                }
                
                masterJobsService.ReleasePartitionLock(job.Id);
                continue;
            }
            
            if (!jobIdByBucketModel.TryGetValue(job.Id, out _)) 
            {
                jobIdByBucketModel.Add(job.Id, bucket!);
            }
        }
        
        var timeRemaining = cutOffTime - DateTime.UtcNow;
        using var batchTimeoutCts = new CancellationTokenSource(timeRemaining > TimeSpan.Zero ? timeRemaining : TimeSpan.FromMilliseconds(100));

        foreach (var job in jobs)
        {
            if (cutOffTime <= DateTime.UtcNow)
            {
                logger.Warn($"Take too long to assign jobs to buckets.", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
                continue;
            }


            if (!jobIdByBucketModel.TryGetValue(job.Id, out var bucket))
            {
                continue;
            }

            logger.Debug($"Assigning job {job.Id} to bucket {bucket.Id}", JobMasterLogSubjectType.Job, job.Id);

            try
            {
                await BackgroundAgentWorker.WorkerClusterOperations.AssignJobToBucketFromHeldOnMasterOrSavePendingAsync(this.BackgroundAgentWorker, job, bucket);
            }
            catch (Exception e)
            {
                logger.Error($"Failed to assign job to bucket. JobId={job.Id}", JobMasterLogSubjectType.Job, job.Id, exception: e);
            }
        }

        masterDistributedLockerService.ReleaseLock(lockKeys.BucketAssignerLock(lockId), lockToken);

        if (BackgroundAgentWorker.IsOnWarmUpTime() && WarmUpInterval < lastScanPlanResult.Interval)
        {
            return  OnTickResult.Success(WarmUpInterval);
        }
        
        return OnTickResult.Success(lastScanPlanResult.Interval);
    }

    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(30);

    private async Task<BucketModel?> GetBucketAvailableForJobAsync(JobRawModel job)
    {
        return await masterBucketsService.SelectBucketAsync(
            JobMasterConstants.BucketFastAllowDiscrepancy,
            job.Priority,
            job.WorkerLane);
    }
}