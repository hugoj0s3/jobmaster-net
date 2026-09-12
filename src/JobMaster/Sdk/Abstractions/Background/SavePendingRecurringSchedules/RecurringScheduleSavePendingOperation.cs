using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Abstractions.Background.SavePendingRecurringSchedules;

internal class RecurringScheduleSavePendingOperation
{
    protected readonly IWorkerClusterOperations workerClusterOperations;
    protected readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    protected readonly IMasterDistributedLockerService distributedLockerService;
    protected readonly IRecentlyInsertedRecurringScheduleQueue recentlyInsertedQueue;
    protected readonly IJobMasterLogger logger;
    private readonly JobMasterLockKeys lockKeys;

    public RecurringScheduleSavePendingOperation(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
    {
        workerClusterOperations = backgroundAgentWorker.GetClusterAwareService<IWorkerClusterOperations>();
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        distributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        recentlyInsertedQueue = backgroundAgentWorker.GetClusterAwareService<IRecentlyInsertedRecurringScheduleQueue>();
        logger = backgroundAgentWorker.GetClusterAwareService<IJobMasterLogger>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
    }

    public async Task SaveRecurringScheduleAsync(RecurringScheduleRawModel schedule)
    {
        if (distributedLockerService.IsLocked(lockKeys.RecurringScheduleCancellingLock(schedule.Id)))
        {
            workerClusterOperations.CancelRecurringSchedule(schedule.Id);
            logger.Debug("Recurring schedule cancelled", JobMasterLogCategory.RecurringSchedule, schedule.Id);
            return;
        }

        schedule.Active();
        await workerClusterOperations.ExecWithRetryAsync(o => o.Upsert(schedule));
        recentlyInsertedQueue.Enqueue(schedule.Id);
    }

    public async Task<SaveDrainResultCode> SaveDrainWithSafeGuardAsync(RecurringScheduleRawModel schedule)
    {
        try
        {
            await SaveRecurringScheduleAsync(schedule);
            return SaveDrainResultCode.Success;
        }
        catch
        {
            try
            {
                await agentJobsDispatcherService.AddSavePendingRecurAsync(schedule);
            }
            catch (Exception e)
            {
                logger.Critical("Failed to add recurring schedule to queue", JobMasterLogCategory.RecurringSchedule, schedule.Id, exception: e);
                return SaveDrainResultCode.Failed;
            }
        }

        return SaveDrainResultCode.Failed;
    }

    public async Task<SaveDrainResultCode> SaveDrainAsync(RecurringScheduleRawModel schedule)
    {
        try
        {
            await SaveRecurringScheduleAsync(schedule);
            return SaveDrainResultCode.Success;
        }
        catch
        {
            logger.Error("Failed to save recurring schedule", JobMasterLogCategory.RecurringSchedule, schedule.Id);
            return SaveDrainResultCode.Failed;
        }
    }
}
