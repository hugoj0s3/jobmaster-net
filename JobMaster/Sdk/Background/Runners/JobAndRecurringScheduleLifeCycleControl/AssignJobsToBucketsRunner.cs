using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
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
            CountLimit = BackgroundAgentWorker.BatchSize,
            Status = JobMasterJobStatus.HeldOnMaster,
            ScheduledTo = utcNow.Add(transientThreshold),
            IsLocked = false,
            Offset = 0,
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
                BackgroundAgentWorker.BatchSize,
                transientThreshold,
                lockerLane:0);
        }
        jobQueryCriteria.CountLimit = lastScanPlanResult.BatchSize;
        
        // Pre-warm bucket cache with fresh data before processing batch and ensure there is at least one available bucket
        var bucketAvailable = await masterBucketsService.SelectBucketAsync(TimeSpan.Zero, null, MasterBucketsService.AnyWorkerLaneKeyword);
        if (bucketAvailable == null)
        {
            logger.Warn("No available bucket found. This is not allowed.", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
            return OnTickResult.Skipped(TimeSpan.FromSeconds(15));
        }
        
        var lockId = JobMasterRandomUtil.GetInt(lastScanPlanResult.LockerMin, lastScanPlanResult.LockerMax + 1);
        
        var lockToken = masterDistributedLockerService.TryLock(lockKeys.BucketAssignerLock(lockId), durationToLock.Add(TimeSpan.FromMinutes(1)));
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromSeconds(10));
        }
        
        var jobIds = await masterJobsService.QueryIdsAsync(jobQueryCriteria);
        if (jobIds.Count <= 0)
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.BucketAssignerLock(lockId), lockToken);
            return OnTickResult.Skipped(lastScanPlanResult.Interval);
        }
        
        var updateResult = masterJobsService.BulkUpdatePartitionLockId(jobIds, lockId, utcNow.Add(durationToLock));
        if (!updateResult)
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.BucketAssignerLock(lockId), lockToken);
            return OnTickResult.Locked(TimeSpan.FromMilliseconds(250));
        }
        
        jobQueryCriteria.IsLocked = true;
        jobQueryCriteria.PartitionLockId = lockId;
        
        var jobs = await masterJobsService.QueryAsync(jobQueryCriteria);
        logger.Debug($"AssignJobsToBucketsRunner: {jobs.Count} jobs found. JobIds: {string.Join(", ", jobIds)}", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
        
        var jobIdByBucketModel = new Dictionary<Guid, BucketModel>(); 
        
        // Assign buckets first because the cache, assign buckets in separate foreach loop.
        foreach (var job in jobs)
        {
            if (job.Status != JobMasterJobStatus.HeldOnMaster)
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
                logger.Warn($"No available bucket found for job {job.Id}. This is not allowed.", JobMasterLogSubjectType.Job, job.Id);
            }
            
            if (bucket is null && job.ScheduledAt <= utcNow.AddMinutes(-30))
            {
                logger.Error($"Job {job.Id} is overdue. Trying any available bucket respecting priority. workerLane: {job.WorkerLane}", JobMasterLogSubjectType.Job, job.Id);
                bucket = await GetBucketAvailableForJobAnyLaneAsync(job);

                if (bucket is null)
                {
                    logger.Error($"Job {job.Id} is overdue. Trying any available bucket. workerLane: {job.WorkerLane} priority: {job.Priority}", JobMasterLogSubjectType.Job, job.Id);
                        
                    bucket = await GetBucketAvailableForJobAnyAsync();
                }
            }
            
            if (bucket is null)
            {
                logger.Warn($"No bucket available for job {job.Id}. WorkerLane={job.WorkerLane} Priority={job.Priority} ScheduledAt={job.ScheduledAt:O}. Clearing partition lock.", JobMasterLogSubjectType.Job, job.Id);
                masterJobsService.ClearPartitionLock(job.Id);
                continue;
            }
            
            if (!jobIdByBucketModel.TryGetValue(job.Id, out _)) 
            {
                jobIdByBucketModel.Add(job.Id, bucket);
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

    private async Task<BucketModel?> GetBucketAvailableForJobAnyLaneAsync(JobRawModel job)
    {
        return await masterBucketsService.SelectBucketAsync(
            JobMasterConstants.BucketFastAllowDiscrepancy,
            job.Priority,
            MasterBucketsService.AnyWorkerLaneKeyword);
    }

    private async Task<BucketModel?> GetBucketAvailableForJobAnyAsync()
    {
        return await masterBucketsService.SelectBucketAsync(
            JobMasterConstants.BucketFastAllowDiscrepancy,
            jobPriority: null,
            workerLane: MasterBucketsService.AnyWorkerLaneKeyword);
    }
}