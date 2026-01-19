using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Background.Runners;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Serialization;
using JobMaster.Sdk.Contracts.Services;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Repositories;
using NATS.Client.JetStream;

namespace JobMaster.NatJetStream.Background;

internal class NetJetStreamSaveRecurringScheduleRunner : NatJetStreamRunnerBase<RecurringScheduleRawModel>, ISaveRecurringSchedulerRunner
{
    private IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private IRecurringSchedulePlanner recurringSchedulePlanner;
    private IWorkerClusterOperations workerClusterOperations;
    
    public NetJetStreamSaveRecurringScheduleRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        masterRecurringSchedulesService = BackgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        workerClusterOperations = backgroundAgentWorker.GetClusterAwareService<IWorkerClusterOperations>();
        recurringSchedulePlanner = backgroundAgentWorker.GetClusterAwareService<IRecurringSchedulePlanner>();
    }

    protected override string GetFullBucketAddressId(string bucketId) => FullBucketAddressIdsUtil.GetRecurringScheduleSavePendingBucketAddress(bucketId);
    protected override bool LostRisk() => true;
    protected override string GetRunnerDescription() => "SaveRecurringSchedule";

    protected override IReadOnlyCollection<BucketStatus> ValidBucketStatuses() => new[] { BucketStatus.Active, BucketStatus.Completing };

    protected override RecurringScheduleRawModel Deserialize(string json)
    {
        return InternalJobMasterSerializer.Deserialize<RecurringScheduleRawModel>(json);
    }

    protected override async Task ProcessPayloadAsync(RecurringScheduleRawModel payload, MsgAckGuard ackGuard)
    {
        try
        {
            if (payload.Status == RecurringScheduleStatus.PendingSave)
            {
                payload.Active();
            }
        
            await workerClusterOperations.ExecWithRetryAsync(o => o.Upsert(payload));
        }
        catch (Exception e)
        {
            this.logger.Error($"{GetRunnerDescription()} - Failed to save recurring schedule", JobMasterLogSubjectType.RecurringSchedule, payload.Id, e);
            throw;
        }

        try
        {
            await recurringSchedulePlanner.ScheduleNextJobsAsync(payload);
        }
        catch (Exception e)
        {
            this.logger.Error($"{GetRunnerDescription()} - Failed to schedule next jobs after save", JobMasterLogSubjectType.RecurringSchedule, payload.Id, e);
        }
    }

    protected override async Task<bool> ShouldAckAfterLockAsync(RecurringScheduleRawModel payload, CancellationToken ct)
    {
        var exists = await masterRecurringSchedulesService.GetAsync(payload.Id);
        return exists is not null;
    }
}