using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
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
    protected readonly IRecentlyInsertedRecurringScheduleQueue recentlyInsertedQueue;
    protected readonly IJobMasterLogger logger;

    public RecurringScheduleSavePendingOperation(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
    {
        workerClusterOperations = backgroundAgentWorker.GetClusterAwareService<IWorkerClusterOperations>();
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        recentlyInsertedQueue = backgroundAgentWorker.GetClusterAwareService<IRecentlyInsertedRecurringScheduleQueue>();
        logger = backgroundAgentWorker.GetClusterAwareService<IJobMasterLogger>();
    }

    public async Task SaveRecurringScheduleAsync(RecurringScheduleRawModel schedule)
    {
        schedule.Active();
        try
        {
            await workerClusterOperations.ExecWithRetryAsync(o => o.Upsert(schedule));
        }
        catch (JobMasterDuplicationException e)
        {
            // Already saved, e.g. by ConfirmOnMasterAsync. Same as the job save path's AlreadyExists;
            // rethrowing would make the runner re-queue it forever. Still enqueued below so it gets the fast path.
            logger.Warn("Recurring schedule duplication detected", JobMasterLogCategory.RecurringSchedule, schedule.Id, exception: e);
        }

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
