using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Abstractions.Background.SavePendingRecurringSchedules;

internal class RecurringScheduleSavePendingOperation
{
    protected readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    protected readonly IRecentlyInsertedRecurringScheduleQueue recentlyInsertedQueue;
    protected readonly IJobMasterLogger logger;
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private readonly string clusterId;

    public RecurringScheduleSavePendingOperation(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
    {
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        recentlyInsertedQueue = backgroundAgentWorker.GetClusterAwareService<IRecentlyInsertedRecurringScheduleQueue>();
        logger = backgroundAgentWorker.GetClusterAwareService<IJobMasterLogger>();
        masterRecurringSchedulesService = backgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        clusterId = backgroundAgentWorker.ClusterConnConfig.ClusterId;
    }

    /// <summary>
    /// Saves one pending-save recurring schedule on master (active, insert-if-not-exists: a schedule already on master
    /// keeps master's copy). Returns whether it is on master now; false means not saved, so the caller keeps its
    /// message.
    /// </summary>
    public async Task<bool> SaveAsync(RecurringScheduleRawModel schedule)
    {
        schedule.Active();
        var result = await masterRecurringSchedulesService.BulkInsertIfNotExistsAsync(new List<RecurringScheduleRawModel> { schedule });
        if (result[BulkInsertStatus.Unknow].Contains(schedule.Id))
        {
            return false;
        }

        // Fast path for planning, also when it was already saved (e.g. by ConfirmOnMasterAsync).
        recentlyInsertedQueue.Enqueue(schedule.Id);
        return true;
    }

    /// <summary>
    /// Saves leased pending-save recurring schedules on master (active, insert-if-not-exists: a schedule already on
    /// master keeps master's copy) and acks the messages of every schedule now on master. A message not acked
    /// (schedule not confirmed on master) is leased again later. Same for a normal and a draining bucket.
    /// Returns false when a partition failed or a schedule wasn't confirmed on master (the others are still saved), so
    /// the runner counts the tick as failed.
    /// </summary>
    public async Task<bool> SaveAndAckAsync(BucketModel currentRunnerBucket, IList<(RecurringScheduleRawModel Schedule, string AckKey)> leasedSchedules)
    {
        var allSaved = true;
        // The same schedule can arrive in more than one message: save it once, ack all of them.
        var ackKeysByScheduleId = leasedSchedules.ToLookup(x => x.Schedule.Id, x => x.AckKey);
        var schedules = leasedSchedules.Select(x => x.Schedule).DistinctBy(x => x.Id).ToList();
        foreach (var schedule in schedules)
        {
            schedule.Active();
        }

        var maxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(clusterId);
        foreach (var partition in schedules.Partition(maxBatchSize))
        {
            try
            {
                var bulkResult = await masterRecurringSchedulesService.BulkInsertIfNotExistsAsync(partition.ToList());

                var savedIds = bulkResult[BulkInsertStatus.Inserted].Concat(bulkResult[BulkInsertStatus.AlreadyInserted]).ToList();
                var ackKeys = savedIds.SelectMany(id => ackKeysByScheduleId[id]).ToList();
                await agentJobsDispatcherService.AckSavePendingRecurAsync(currentRunnerBucket.AgentConnectionId, currentRunnerBucket.Id, ackKeys);

                // Fast path for planning, as before for a saved (or already saved) schedule.
                foreach (var id in savedIds)
                {
                    recentlyInsertedQueue.Enqueue(id);
                }

                if (bulkResult[BulkInsertStatus.Unknow].Count > 0)
                {
                    allSaved = false;
                }
            }
            catch (Exception e)
            {
                // The partition's unacked messages are leased again; continue with the rest of the batch.
                logger.Error($"Failed to save a partition of {partition.Count} pending recurring schedule(s) for bucket {currentRunnerBucket.Id}. Continuing with the remaining partitions.",
                    JobMasterLogCategory.Bucket, currentRunnerBucket.Id, exception: e);
                allSaved = false;
            }
        }

        return allSaved;
    }
}
