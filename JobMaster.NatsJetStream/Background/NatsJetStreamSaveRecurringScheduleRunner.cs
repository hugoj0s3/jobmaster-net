using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingRecurringSchedules;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Repositories;
using NATS.Client.JetStream;

namespace JobMaster.NatsJetStream.Background;

internal class NatsJetStreamSaveRecurringScheduleRunner : NatsJetStreamRunnerBase<RecurringScheduleRawModel>, ISaveRecurringSchedulerRunner
{
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private readonly IRecurringSchedulePlanner recurringSchedulePlanner;
    private readonly IMasterDistributedLockerService distributedLockerService;
    private readonly JobMasterLockKeys lockKeys;
    private readonly RecurringScheduleSavePendingOperation savePendingOperation;
    
    public NatsJetStreamSaveRecurringScheduleRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        masterRecurringSchedulesService = BackgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        recurringSchedulePlanner = backgroundAgentWorker.GetClusterAwareService<IRecurringSchedulePlanner>();
        distributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
        savePendingOperation = new RecurringScheduleSavePendingOperation(backgroundAgentWorker);
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
            await savePendingOperation.SaveRecurringScheduleAsync(payload);
        }
        catch (Exception e)
        {
            this.logger.Error($"{GetRunnerDescription()} - Failed to save recurring schedule", JobMasterLogCategory.RecurringSchedule, payload.Id, e);
            throw;
        }
    }

    protected override async Task<bool> ShouldAckAfterLockAsync(RecurringScheduleRawModel payload, CancellationToken ct)
    {
        var exists = await masterRecurringSchedulesService.GetAsync(payload.Id);
        return exists is not null;
    }
}