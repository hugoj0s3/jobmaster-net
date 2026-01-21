using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

internal class ManualDrainSavePendingRecurringScheduleRunner : BucketAwareRunner, IDrainSavePendingRecurringScheduleRunner
{
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    private readonly IMasterBucketsService masterBucketsService;

    private int failedSavedCountConsecutive = 0;

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(3);

    public ManualDrainSavePendingRecurringScheduleRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        masterRecurringSchedulesService = backgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
    }

    public void DefineBucketId(string bucketId)
    {
        BucketId = bucketId;
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (string.IsNullOrEmpty(BucketId))
        {
            return OnTickResult.Skipped(this);
        }
        
        var bucket = masterBucketsService.Get(BucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket == null || bucket.Status != BucketStatus.Draining)
        {
            return OnTickResult.Skipped(this);
        }

        var recurringSchedules = await agentJobsDispatcherService.DequeueSavePendingRecurAsync(
            BackgroundAgentWorker.AgentConnectionId,
            BucketId!,
            BackgroundAgentWorker.BatchSize);

        if (recurringSchedules.Count <= 0)
        {
            return OnTickResult.Skipped(TimeSpan.FromMinutes(1));
        }

        bool hasFailed = false;
        foreach (var recurringScheduleRawModel in recurringSchedules)
        {
            try
            {
                if (recurringScheduleRawModel.Status == RecurringScheduleStatus.PendingSave)
                {
                    recurringScheduleRawModel.Active();
                }

                await masterRecurringSchedulesService.UpsertAsync(recurringScheduleRawModel);
            }
            catch
            {
                logger.Error("Failed to save recurring schedule", JobMasterLogSubjectType.RecurringSchedule, recurringScheduleRawModel.Id);
                hasFailed = true;

                try
                {
                    await agentJobsDispatcherService.AddSavePendingRecurAsync(recurringScheduleRawModel);
                }
                catch
                {
                    logger.Critical($"Failed to add recurring schedule to queue. Data: {InternalJobMasterSerializer.Serialize(recurringScheduleRawModel)}", JobMasterLogSubjectType.RecurringSchedule, recurringScheduleRawModel.Id);
                }
            }
        }

        if (hasFailed)
        {
            failedSavedCountConsecutive++;
            var secondsToWait = Math.Min(60, 10 + failedSavedCountConsecutive * 5);
            return OnTickResult.Skipped(TimeSpan.FromSeconds(secondsToWait));
        }

        failedSavedCountConsecutive = 0;
        return OnTickResult.Success(this);
    }
}
