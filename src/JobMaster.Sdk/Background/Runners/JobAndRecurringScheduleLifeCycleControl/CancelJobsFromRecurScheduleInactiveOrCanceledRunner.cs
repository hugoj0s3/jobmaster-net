using JobMaster.Contracts.Models;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Background.ScanPlans;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

public class CancelJobsFromRecurScheduleInactiveOrCanceledRunner : JobMasterRunner
{
    private IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private IMasterClusterConfigurationService masterClusterConfigurationService;
    private ScanPlanResult? lastScanPlanResult;
    private JobMasterLockKeys lockKeys;
    private IMasterDistributedLockerService distributedLockerService;
    private IMasterJobsService masterJobsService;
    
    public CancelJobsFromRecurScheduleInactiveOrCanceledRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker, bucketAwareLifeCycle: true, useSemaphore: true)
    {
        masterRecurringSchedulesService = backgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        masterClusterConfigurationService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
        distributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        var configuration = masterClusterConfigurationService.Get();
        var transientThreshold = configuration?.TransientThreshold ?? TimeSpan.FromMinutes(5);

        var recurringScheduleQueryCriteria = new RecurringScheduleQueryCriteria()
        {
            CountLimit = BackgroundAgentWorker.BatchSize,
            CanceledOrInactive = true,
            IsJobCancellationPending = true, 
            IsLocked = false,
            Offset = 0,
        };
        
        if (lastScanPlanResult == null || lastScanPlanResult.ShouldCalculateAgain())
        {
            var count = masterRecurringSchedulesService.Count(recurringScheduleQueryCriteria);
            var workerCount = await BackgroundAgentWorker.WorkerClusterOperations.CountActiveCoordinatorWorkersAsync();
            if (workerCount <= 0)
            {
                workerCount = 1;
            }
            
            lastScanPlanResult = ScanPlanner.ComputeScanPlanHalfWindow(
                count,
                workerCount,
                BackgroundAgentWorker.BatchSize,
                transientThreshold,
                lockerLane:3);
        }
        var utcNow = DateTime.UtcNow;
        var durationToLock = JobMasterConstants.DurationToLockRecords;
        var cutOffTime = utcNow.Add(durationToLock).AddSeconds(-30);
        
        var lockId = JobMasterRandomUtil.GetInt(lastScanPlanResult.LockerMin, lastScanPlanResult.LockerMax + 1);
        
        var lockToken = distributedLockerService.TryLock(lockKeys.RecurringSchedulerLock(lockId), durationToLock.Add(TimeSpan.FromMinutes(1)));
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromSeconds(10));
        }
        
        var recurringScheduleIds = await masterRecurringSchedulesService.QueryIdsAsync(recurringScheduleQueryCriteria);
        if (recurringScheduleIds.Count <= 0)
        {
            distributedLockerService.ReleaseLock(lockKeys.RecurringSchedulerLock(lockId), lockToken);
            return OnTickResult.Skipped(TimeSpan.FromMinutes(2));
        }
        
        var bulkUpdateResult = masterRecurringSchedulesService.BulkUpdatePartitionLockId(recurringScheduleIds, lockId, utcNow.Add(durationToLock));
        if (!bulkUpdateResult)
        {
            distributedLockerService.ReleaseLock(lockKeys.RecurringSchedulerLock(lockId), lockToken);
            return OnTickResult.Locked(TimeSpan.FromMilliseconds(250));
        }
        
        recurringScheduleQueryCriteria.IsLocked = true;
        recurringScheduleQueryCriteria.PartitionLockId = lockId;
        
        var recurringSchedules = await masterRecurringSchedulesService.QueryAsync(recurringScheduleQueryCriteria);
        foreach (var recurringSchedule in recurringSchedules)
        {
            if (cutOffTime <= DateTime.UtcNow)
            {
                logger.Warn($"Runner timeout {durationToLock}", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
                break;
            }
            
            if (ct.IsCancellationRequested)
            {
                break;
            }

            CencelJobs(recurringSchedule, lockId, durationToLock);
        }
        
        distributedLockerService.ReleaseLock(lockKeys.RecurringSchedulerLock(lockId), lockToken);
        
        return OnTickResult.Success(lastScanPlanResult.Interval);
    }

    private void CencelJobs(RecurringScheduleRawModel recurringScheduleRawModel, int lockId, TimeSpan durationToLock)
    {
        var jobQueryCriteria = new JobQueryCriteria()
        {
            CountLimit = BackgroundAgentWorker.BatchSize,
            RecurringScheduleId = recurringScheduleRawModel.Id,
            // Only cancel jobs scheduled 5 minutes later. the job on fly will be cancelled by the JobExecutionEngine.
            ScheduledFrom = DateTime.UtcNow.AddMinutes(5),
            IsLocked = false,
            Offset = 0,
        };
        
        var jobs = masterJobsService.Query(jobQueryCriteria);
        var jobIdsToLock = jobs
            .Where(x => !x.Status.IsFinalStatus())
            .Select(job => job.Id).ToList();
        var updateResult = masterJobsService.BulkUpdatePartitionLockId(jobIdsToLock, lockId, DateTime.UtcNow.Add(durationToLock));
        if (!updateResult)
        {
            return;
        }
        
        jobQueryCriteria.IsLocked = true;
        jobQueryCriteria.PartitionLockId = lockId;
        
        var jobIdsToCancel = masterJobsService.QueryIds(jobQueryCriteria);
        
        var finalStatuses = JobMasterJobStatusUtil.GetFinalStatuses().Where(x => x != JobMasterJobStatus.Cancelled).ToList();
        masterJobsService.BulkUpdateStatus(jobIdsToCancel, JobMasterJobStatus.Cancelled, null, null, null, excludeStatuses: finalStatuses);

        recurringScheduleRawModel.HasCancelJobsFinish();
        masterRecurringSchedulesService.Upsert(recurringScheduleRawModel);
    }
    

    public override TimeSpan SucceedInterval => TimeSpan.FromMinutes(1);
}