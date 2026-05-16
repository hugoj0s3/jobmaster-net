using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.ScanPlans;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

/// <summary>
/// Bulk-cancels future pending jobs for recurring schedules that have been inactivated or
/// cancelled and have <c>IsJobCancellationPending</c> set. Only jobs scheduled more than
/// 5 minutes in the future are cancelled; in-flight jobs are handled by the execution engine.
/// Uses a scan-plan with slot-based distributed locking to coordinate across multiple
/// coordinator workers.
/// Runs approximately every <see cref="SucceedInterval"/>, adjusted by the scan plan.
/// </summary>
internal class CancelJobsFromRecurScheduleInactiveOrCanceledRunner : JobMasterRunner
{
    private IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private IMasterClusterConfigurationService masterClusterConfigurationService;
    private ScanPlanResult? lastScanPlanResult;
    private JobMasterLockKeys lockKeys;
    private IMasterDistributedLockerService distributedLockerService;
    private IMasterJobsService masterJobsService;
    
    public CancelJobsFromRecurScheduleInactiveOrCanceledRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
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
            CountLimit = BackgroundAgentWorker.TransferBatchSize,
            CanceledOrInactive = true,
            IsJobCancellationPending = true, 
            IsLocked = false,
            Offset = 0,
            SortBy = new SortByCriteria()
            {
                Property = nameof(RecurringScheduleRawModel.LastPlanCoverageUntil),
                Ascending = false,
            }
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
                BackgroundAgentWorker.TransferBatchSize,
                transientThreshold,
                lockerLane:3);
        }
        var utcNow = DateTime.UtcNow;
        var durationToLock = JobMasterConstants.DurationToLockRecords;
        var cutOffTime = utcNow.Add(durationToLock).AddSeconds(-30);
        
        var lockSlot = JobMasterRandomUtil.GetInt(lastScanPlanResult.LockerMin, lastScanPlanResult.LockerMax + 1);
        var lockToken = distributedLockerService.TryLock(lockKeys.RecurringSchedulerLock(lockSlot), durationToLock.Add(TimeSpan.FromMinutes(1)));
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromSeconds(10));
        }

        var recurringSchedules = await masterRecurringSchedulesService.AcquireAndFetchAsync(recurringScheduleQueryCriteria, utcNow.Add(durationToLock));
        if (recurringSchedules.Count <= 0)
        {
            distributedLockerService.ReleaseLock(lockKeys.RecurringSchedulerLock(lockSlot), lockToken);
            return OnTickResult.Skipped(TimeSpan.FromMinutes(2));
        }
        
        foreach (var recurringSchedule in recurringSchedules)
        {
            if (cutOffTime <= DateTime.UtcNow)
            {
                logger.Warn($"Runner timeout {durationToLock}", JobMasterLogCategory.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
                break;
            }
            
            if (ct.IsCancellationRequested)
            {
                break;
            }

            await CencelJobsAsync(recurringSchedule, lockSlot, durationToLock, ct);
        }
        
        distributedLockerService.ReleaseLock(lockKeys.RecurringSchedulerLock(lockSlot), lockToken);
        
        return OnTickResult.Success(lastScanPlanResult.Interval);
    }

    private async Task CencelJobsAsync(RecurringScheduleRawModel recurringScheduleRawModel, int lockId, TimeSpan durationToLock, CancellationToken ct)
    {
        if (distributedLockerService.IsLocked(lockKeys.RecurringSchedulePlan(recurringScheduleRawModel.Id)))
        {
            return;
        }
        
        var jobQueryCriteria = new JobQueryCriteria()
        {
            CountLimit = BackgroundAgentWorker.TransferBatchSize,
            SourceId = recurringScheduleRawModel.Id,
            TriggerSourceTypes = new List<JobMasterTriggerSourceType>
            {
                recurringScheduleRawModel.RecurringScheduleType == RecurringScheduleType.Static
                    ? JobMasterTriggerSourceType.StaticRecurring
                    : JobMasterTriggerSourceType.DynamicRecurring
            },
            // Only cancel jobs scheduled 5 minutes later. the job on fly will be cancelled by the JobExecutionEngine.
            NextPlanExecutionAtFrom = DateTime.UtcNow.AddMinutes(5),
            Offset = 0,
            SortBy = new SortByCriteria()
            {
                Property = nameof(JobRawModel.NextPlanExecutionAt),
                Ascending = true,
            },
        };

        if (ct.IsCancellationRequested)
        {
            return;
        }

        var expiresAtUtc = DateTime.UtcNow.Add(durationToLock);
        var jobs = await masterJobsService.AcquireAndFetchAsync(jobQueryCriteria, expiresAtUtc);
        var jobIdsToCancel = jobs
            .Where(x => !x.Status.IsFinalStatus())
            .Select(x => x.Id)
            .ToList();

        if (jobIdsToCancel.Count <= 0)
        {
            recurringScheduleRawModel.HasCancelJobsFinish();
            await masterRecurringSchedulesService.UpdateAsync(recurringScheduleRawModel);
            return;
        }
        
        var bulkUpdateRequest = BulkJobUpdateRequest.Cancel(jobIdsToCancel);
        await masterJobsService.BulkUpdateAsync(bulkUpdateRequest);

        recurringScheduleRawModel.HasCancelJobsFinish();
        await masterRecurringSchedulesService.UpdateAsync(recurringScheduleRawModel);
    }
    

    public override TimeSpan SucceedInterval => TimeSpan.FromMinutes(1);
}