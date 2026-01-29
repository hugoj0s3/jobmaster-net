using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.ScanPlans;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

internal class ScheduleRecurringJobsRunner : JobMasterRunner
{
    private IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private IMasterClusterConfigurationService masterClusterConfigurationService;
    private IRecurringSchedulePlanner recurringSchedulePlanner;
    private ScanPlanResult? lastScanPlanResult;
    private JobMasterLockKeys lockKeys;
    private IMasterDistributedLockerService distributedLockerService;
    
    public ScheduleRecurringJobsRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
        masterRecurringSchedulesService = backgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        masterClusterConfigurationService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
        recurringSchedulePlanner = backgroundAgentWorker.GetClusterAwareService<IRecurringSchedulePlanner>();
        distributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        var configuration = masterClusterConfigurationService.Get();
        if (configuration?.ClusterMode != ClusterMode.Active)
        {
            return OnTickResult.Skipped(TimeSpan.FromMinutes(2));
        }
        
        var transientThreshold = configuration?.TransientThreshold ?? TimeSpan.FromMinutes(5);

        var utcNow = DateTime.UtcNow;
        var durationToLock = JobMasterConstants.DurationToLockRecords;
        var cutOffTime = utcNow.Add(durationToLock).AddSeconds(-30);
        
        var recurringScheduleQueryCriteria = new RecurringScheduleQueryCriteria()
        {
            CountLimit = BackgroundAgentWorker.BatchSize,
            Status = RecurringScheduleStatus.Active,
            CoverageUntil = utcNow.Add(transientThreshold),
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
                lockerLane:2);
        }
        recurringScheduleQueryCriteria.CountLimit = lastScanPlanResult.BatchSize;
        
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
        
        var updateResult = masterRecurringSchedulesService.BulkUpdatePartitionLockId(recurringScheduleIds, lockId, utcNow.Add(durationToLock));
        if (!updateResult)
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
                break;
            }
            
            if (ct.IsCancellationRequested)
            {
                break;
            }

            await recurringSchedulePlanner.ScheduleNextJobsAsync(recurringSchedule);
        }
        
        distributedLockerService.ReleaseLock(lockKeys.RecurringSchedulerLock(lockId), lockToken);
        
        return OnTickResult.Success(lastScanPlanResult.Interval);
    }

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(10);
}