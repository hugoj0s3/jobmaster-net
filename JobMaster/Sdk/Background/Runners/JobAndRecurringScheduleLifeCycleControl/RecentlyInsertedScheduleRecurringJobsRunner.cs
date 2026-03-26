using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.ScanPlans;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

internal class RecentlyInsertedScheduleRecurringJobsRunner : JobMasterRunner
{
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private readonly IRecurringSchedulePlanner recurringSchedulePlanner;
    private readonly IRecentlyInsertedRecurringScheduleQueue queue;
    private readonly IMasterDistributedLockerService distributedLockerService;
    private readonly JobMasterLockKeys lockKeys;

    private const int MinPartitionLockId = 5 * ScanPlanner.LaneWidth + 1;

    public RecentlyInsertedScheduleRecurringJobsRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        masterRecurringSchedulesService = backgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        recurringSchedulePlanner = backgroundAgentWorker.GetClusterAwareService<IRecurringSchedulePlanner>();
        queue = backgroundAgentWorker.GetClusterAwareService<IRecentlyInsertedRecurringScheduleQueue>();
        distributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        var countExecutor = await BackgroundAgentWorker.WorkerClusterOperations.CountActiveExecutorWorkersAsync();
        var maxPartitionLockId = MinPartitionLockId + countExecutor + 1;
        var partitionLockId = JobMasterRandomUtil.GetInt(MinPartitionLockId, maxPartitionLockId);
        var ids = queue.Dequeue(BackgroundAgentWorker.TransferBatchSize);
        if (ids.Count == 0)
        {
            return OnTickResult.Skipped(TimeSpan.FromSeconds(1));
        }

        var utcNow = DateTime.UtcNow;
        var durationToLock = JobMasterConstants.DurationToLockRecords;

        var lockToken = distributedLockerService.TryLock(
            lockKeys.RecurringSchedulerLock(partitionLockId),
            durationToLock.Add(TimeSpan.FromMinutes(1)));

        if (lockToken == null)
        {
            foreach (var id in ids)
            {
                queue.Enqueue(id);
            }
            return OnTickResult.Locked(TimeSpan.FromSeconds(1));
        }

        var recurringSchedules = await masterRecurringSchedulesService.AcquireAndFetchByIdsAsync(
            ids.ToList(),
            MinPartitionLockId,
            utcNow.Add(durationToLock));

        distributedLockerService.ReleaseLock(lockKeys.RecurringSchedulerLock(MinPartitionLockId), lockToken);

        foreach (var schedule in recurringSchedules)
        {
            if (ct.IsCancellationRequested)
            {
                break;
            }

            await recurringSchedulePlanner.ScheduleNextJobsAsync(schedule);
        }

        logger.Debug($"Recently inserted recurring schedules planned: {recurringSchedules.Count}/{ids.Count}", JobMasterLogSubjectType.RecurringSchedule);

        return OnTickResult.Success(this);
    }

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(1);
}
