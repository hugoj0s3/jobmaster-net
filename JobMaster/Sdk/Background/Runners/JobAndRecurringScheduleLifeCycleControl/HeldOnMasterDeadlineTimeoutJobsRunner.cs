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
/// Safety net runner that recovers jobs whose ProcessDeadline has expired and are no longer held by an active bucket.
/// This is not the primary processing path — it only intervenes when the drain process fails or takes too long.
/// </summary>
/// <remarks>
/// <para><strong>Execution Interval:</strong> Dynamic (calculated based on job count and worker threads)</para>
/// <para><strong>Lifecycle:</strong> Global runner (bucketAwareLifeCycle: false, useSemaphore: true)</para>
/// <para><strong>Key Operations:</strong></para>
/// <list type="bullet">
/// <item>Queries active and completing bucket IDs upfront and excludes their jobs from intervention</item>
/// <item>Only acts on jobs whose bucket is gone, lost, or otherwise inactive</item>
/// <item>Marks eligible jobs as HeldOnMaster for reassignment</item>
/// <item>Uses partition locking to prevent race conditions across concurrent workers</item>
/// </list>
/// <para><strong>Safety Features:</strong></para>
/// <list type="bullet">
/// <item>Never touches jobs belonging to Active or Completing buckets — those are owned by their worker</item>
/// <item>Skips jobs already in a final status (completed, failed, cancelled)</item>
/// <item>Uses database-level bucket exclusion for efficient and safe filtering</item>
/// </list>
/// <para><strong>Performance:</strong> Uses partition locking similar to AssignHeldJobsRunner for scalable concurrent processing</para>
/// </remarks>
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
                TimeSpan.FromMinutes(2),
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
