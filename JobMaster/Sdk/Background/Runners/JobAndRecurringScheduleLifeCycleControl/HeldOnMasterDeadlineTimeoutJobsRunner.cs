using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.ScanPlans;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

/// <summary>
/// Detects jobs whose <c>ProcessDeadline</c> has expired while they are held on master
/// (i.e. not currently owned by an active or completing bucket) and bulk-updates them
/// to the <c>HeldOnMaster</c> status so the scheduler can re-evaluate and re-assign them.
/// Uses a scan-plan with slot-based distributed locking to coordinate safely across
/// multiple coordinator workers. Active-bucket jobs are always excluded from the update.
/// Runs approximately every <see cref="SucceedInterval"/>, adjusted by the scan plan.
/// </summary>
internal class HeldOnMasterDeadlineTimeoutJobsRunner : JobMasterRunner
{
    private readonly IMasterJobsService masterJobsService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly IMasterBucketsService masterBucketsService;
    
    private ScanPlanResult? lastScanPlanResult;
    
    private readonly JobMasterLockKeys lockKeys;
    
    public override TimeSpan SucceedInterval => TimeSpan.FromMinutes(1);
    
    public HeldOnMasterDeadlineTimeoutJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) 
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
    }
    
    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (BackgroundAgentWorker.StopRequested)
        {
            return OnTickResult.Skipped(this);
        }

        var utcNow = DateTime.UtcNow;
        var durationToLock = JobMasterConstants.DurationToLockRecords;
        var cutOffTime = utcNow.Add(durationToLock).AddSeconds(-30);

        var bucketQueryCriteria = new MasterBucketQueryCriteria()
        {
            Statuses = new List<BucketStatus>()
            {
                BucketStatus.Active,
                BucketStatus.Completing
            }
        };

        var activeBuckets = await masterBucketsService.QueryAsync(bucketQueryCriteria, JobMasterConstants.BucketFastAllowDiscrepancy);
        var activeBucketIds = activeBuckets.Select(b => b.Id).ToList();
        
        var jobQueryCriteria = new JobQueryCriteria()
        {
            CountLimit = BackgroundAgentWorker.TransferBatchSize,
            ProcessDeadlineTo = JobMasterConstants.NowUtcWithSkewTolerance(),
            Offset = 0,
            ExcludeBucketIds = activeBucketIds,
            SortBy = new SortByCriteria()
            {
                Property = nameof(JobRawModel.ProcessDeadline),
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
                JobMasterConstants.JobProcessDeadlineDefaultDuration,
                lockerLane:1);
        }
        jobQueryCriteria.CountLimit = lastScanPlanResult.BatchSize;
        
        var lockSlot = JobMasterRandomUtil.GetInt(lastScanPlanResult.LockerMin, lastScanPlanResult.LockerMax + 1);
        
        var lockToken = masterDistributedLockerService.TryLock(lockKeys.ProcessDeadlineTimeoutLock(lockSlot), durationToLock.Add(TimeSpan.FromMinutes(1)));
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromSeconds(10));
        }

        var jobs = await masterJobsService.AcquireAndFetchAsync(jobQueryCriteria, utcNow.Add(durationToLock));
        if (jobs.Count <= 0)
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.ProcessDeadlineTimeoutLock(lockSlot), lockToken);
            return OnTickResult.Skipped(TimeSpan.FromMinutes(2));
        }

        var eligibleJobs = jobs
            .Where(j => !j.Status.IsFinalStatus())
            .ToList();

        if (eligibleJobs.Count < jobs.Count)
        {
            logger.Warn($"Skipping {jobs.Count - eligibleJobs.Count} final-status jobs.", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
        }

        logger.Info($"HeldOnMasterDeadlineTimeoutJobsRunner: Marking {eligibleJobs.Count} jobs as HeldOnMaster. JobIds: {string.Join(", ", eligibleJobs.Select(x => x.Id).Take(10))}", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);

        var partitions = eligibleJobs.Select(j => j.Id).ToList().Partition(JobMasterConstants.MaxBatchSizeForBulkOperation);
        foreach (var partition in partitions)
        {
            if (ct.IsCancellationRequested || cutOffTime <= DateTime.UtcNow)
            {
                logger.Warn($"Runner timeout or cancellation — stopping bulk update early.", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
                break;
            }

            await masterJobsService.BulkUpdateAsync(BulkJobUpdateRequest.HeldOnMaster(partition.ToList()));
        }

        masterDistributedLockerService.ReleaseLock(lockKeys.ProcessDeadlineTimeoutLock(lockSlot), lockToken);

        return OnTickResult.Success(lastScanPlanResult.Interval);
    }
}
