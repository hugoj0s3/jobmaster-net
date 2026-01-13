using JobMaster.Contracts.Models;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Background.Runners;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Serialization;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Repositories;
using NATS.Client.JetStream;

namespace JobMaster.NatJetStreams.Background;

internal sealed class NatJetStreamDrainSavePendingRecurringScheduleRunner
    : NatJetStreamRunnerBase<RecurringScheduleRawModel>, IDrainSavePendingRecurringScheduleRunner
{
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;

    public NatJetStreamDrainSavePendingRecurringScheduleRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker)
    {
        masterRecurringSchedulesService = backgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
    }

    protected override string GetFullBucketAddressId(string bucketId)
        => FullBucketAddressIdsUtil.GetRecurringScheduleSavePendingBucketAddress(bucketId);

    protected override bool LostRisk() => true;

    protected override string GetRunnerDescription() => "SavePendingRecurringSchedule";

    protected override IReadOnlyCollection<BucketStatus> ValidBucketStatuses()
        => new[] { BucketStatus.Draining };

    protected override RecurringScheduleRawModel Deserialize(string json)
        => InternalJobMasterSerializer.Deserialize<RecurringScheduleRawModel>(json);

    protected override async Task ProcessPayloadAsync(RecurringScheduleRawModel recurring, MsgAckGuard ackGuard)
    {
        if (recurring.Status == RecurringScheduleStatus.PendingSave)
        {
            recurring.Active();
        }
        await BackgroundAgentWorker.WorkerClusterOperations
            .ExecWithRetryAsync(o => o.Upsert(recurring));
    }

    protected override async Task<bool> ShouldAckAfterLockAsync(RecurringScheduleRawModel payload, CancellationToken ct)
    {
        var existing = await masterRecurringSchedulesService.GetAsync(payload.Id);
        return existing is not null;
    }
    
    protected override TimeSpan DelayAfterProcessPayload() => 
        this.BackgroundAgentWorker.Mode == AgentWorkerMode.Drain ? TimeSpan.FromMilliseconds(50) : TimeSpan.FromMilliseconds(250);
    
    protected override TimeSpan LongDelayAfterBatchSize() => 
        this.BackgroundAgentWorker.Mode == AgentWorkerMode.Drain ? TimeSpan.FromMilliseconds(250) : TimeSpan.FromMilliseconds(1000);
}
