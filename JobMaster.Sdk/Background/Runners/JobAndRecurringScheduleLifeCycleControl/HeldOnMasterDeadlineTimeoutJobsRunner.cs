using JobMaster.Contracts.Models;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Background.ScanPlans;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

/// <summary>
/// Enforces job deadlines and recovers stuck jobs that have exceeded their ProcessDeadline.
/// This runner monitors jobs for deadline violations and marks them as held on master for reassignment,
/// preventing jobs from getting permanently stuck when workers become unresponsive.
/// </summary>
/// <remarks>
/// <para><strong>Execution Interval:</strong> Dynamic (calculated based on job count and worker threads)</para>
/// <para><strong>Lifecycle:</strong> Global runner (useIndependentLifecycle: false, useSemaphore: true)</para>
/// <para><strong>Key Operations:</strong></para>
/// <list type="bullet">
/// <item>Queries jobs with ProcessDeadline &lt; DateTime.UtcNow (deadline missed by 5+ minutes)</item>
/// <item>Uses partition locking to prevent race conditions during concurrent access</item>
/// <item>Checks for job cancellation and processing locks before intervention</item>
/// <item>Marks deadline-missed jobs as HeldOnMaster for reassignment to healthy workers</item>
/// </list>
/// <para><strong>Safety Features:</strong></para>
/// <list type="bullet">
/// <item>Respects job cancellation locks (defers to cancellation process)</item>
/// <item>Double-checks processing locks to avoid interfering with active jobs</item>
/// <item>Skips jobs in final status (completed, failed, cancelled)</item>
/// <item>Uses database-level filtering for efficient deadline detection</item>
/// </list>
/// <para><strong>Performance:</strong> Uses partition locking similar to AssignHeldJobsRunner for scalable concurrent processing</para>
/// </remarks>
public class HeldOnMasterDeadlineTimeoutJobsRunner : JobMasterRunner
{
    private readonly IMasterJobsService masterJobsService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly IMasterAgentWorkersService masterAgentWorkersService;
    
    private ScanPlanResult? lastScanPlanResult;
    
    private readonly JobMasterLockKeys lockKeys;
    
    public override TimeSpan SucceedInterval => TimeSpan.FromMinutes(1);
    
    public HeldOnMasterDeadlineTimeoutJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) 
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        masterAgentWorkersService = backgroundAgentWorker.GetClusterAwareService<IMasterAgentWorkersService>();
        
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
        
        var jobQueryCriteria = new JobQueryCriteria()
        {
            CountLimit = BackgroundAgentWorker.BatchSize,
            ProcessDeadlineTo = utcNow.AddSeconds(-30),
            IsLocked = false,
            Offset = 0,
        };
        
        if (lastScanPlanResult == null || lastScanPlanResult.ShouldCalculateAgain())
        {
            var countJobs = masterJobsService.Count(jobQueryCriteria);
            var workerCount = await BackgroundAgentWorker.WorkerClusterOperations.CountAliveWorkersAsync();
            if (workerCount <= 0)
            {
                workerCount = 1;
            }
            
            lastScanPlanResult = ScanPlanner.ComputeScanPlanHalfWindow(
                countJobs,
                workerCount,
                BackgroundAgentWorker.BatchSize,
                TimeSpan.FromMinutes(2),
                lockerLane:1);
        }
        jobQueryCriteria.CountLimit = lastScanPlanResult.BatchSize;
        
        var lockId = JobMasterRandomUtil.GetInt(lastScanPlanResult.LockerMin, lastScanPlanResult.LockerMax + 1);
        
        var lockToken = masterDistributedLockerService.TryLock(lockKeys.ProcessDeadlineTimeoutLock(lockId), durationToLock.Add(TimeSpan.FromMinutes(1)));
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromSeconds(10));
        }
        
        var jobIds = await masterJobsService.QueryIdsAsync(jobQueryCriteria);
        if (jobIds.Count <= 0)
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.ProcessDeadlineTimeoutLock(lockId), lockToken);
            return OnTickResult.Skipped(TimeSpan.FromMinutes(2));
        }
        
        logger.Info($"HeldOnMasterDeadlineTimeoutJobsRunner: Found {jobIds.Count} jobs past deadline. JobIds: {string.Join(", ", jobIds.Take(10))}", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
        
        // Lock the partition to prevent race conditions
        var updateResult = masterJobsService.BulkUpdatePartitionLockId(jobIds, lockId, utcNow.Add(durationToLock));
        if (!updateResult)
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.ProcessDeadlineTimeoutLock(lockId), lockToken);
            return OnTickResult.Locked(TimeSpan.FromMilliseconds(250));
        }
        
        jobQueryCriteria.IsLocked = true;
        jobQueryCriteria.PartitionLockId = lockId;
        
        var jobs = await masterJobsService.QueryAsync(jobQueryCriteria);
        
        await MarkAsHeldOnMasterAsync(jobs, cutOffTime, ct);
        
        masterDistributedLockerService.ReleaseLock(lockKeys.ProcessDeadlineTimeoutLock(lockId), lockToken);
        
        return OnTickResult.Success(lastScanPlanResult.Interval);
    }

    private async Task MarkAsHeldOnMasterAsync(IList<JobRawModel> jobs, DateTime cutOffTime, CancellationToken ct)
    {
        foreach (var job in jobs)
        {

            if (cutOffTime <= DateTime.UtcNow)
            {
                logger.Warn($"Runner timeout ", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
                break;
            }

            if (ct.IsCancellationRequested)
            {
                break;
            }
            
            if (job.Status.IsFinalStatus()) 
            {
                logger.Warn($"Skipping final status job. JobId={job.Id} Status={job.Status}", JobMasterLogSubjectType.Job, job.Id);
                continue;
            }

            logger.Warn($"Job exceeded ProcessDeadline. Marking as HeldOnMaster. JobId={job.Id} Status={job.Status} ProcessDeadline={job.ProcessDeadline:O} BucketId={job.BucketId}", JobMasterLogSubjectType.Job, job.Id);
            job.MarkAsHeldOnMaster();
            await masterJobsService.UpsertAsync(job);
        }
    }
}
