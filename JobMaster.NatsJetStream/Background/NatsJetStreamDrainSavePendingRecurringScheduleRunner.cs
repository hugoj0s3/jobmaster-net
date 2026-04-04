using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingRecurringSchedules;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Repositories;
using NATS.Client.JetStream;

namespace JobMaster.NatsJetStream.Background;

internal sealed class NatsJetStreamDrainSavePendingRecurringScheduleRunner
    : NatsJetStreamRunnerBase<RecurringScheduleRawModel>, IDrainSavePendingRecurringScheduleRunner
{
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private readonly RecurringScheduleSavePendingOperation savePendingOperation;

    public NatsJetStreamDrainSavePendingRecurringScheduleRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker)
    {
        masterRecurringSchedulesService = backgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        savePendingOperation = new RecurringScheduleSavePendingOperation(backgroundAgentWorker);
    }

    protected override string GetFullBucketAddressId(string bucketId)
        => FullBucketAddressIdsUtil.GetRecurringScheduleSavePendingBucketAddress(bucketId);

    protected override bool LostRisk() => true;

    protected override string GetRunnerDescription() => "SavePendingRecurringSchedule";

    protected override IReadOnlyCollection<BucketStatus> ValidBucketStatuses()
        => new[] { BucketStatus.Draining };

    protected override RecurringScheduleRawModel Deserialize(string json)
        => InternalJobMasterSerializer.Deserialize<RecurringScheduleRawModel>(json);

    protected override Task ProcessPayloadAsync(RecurringScheduleRawModel recurring, MsgAckGuard ackGuard)
        => savePendingOperation.SaveRecurringScheduleAsync(recurring);

    protected override async Task<bool> ShouldAckAfterLockAsync(RecurringScheduleRawModel payload, CancellationToken ct)
    {
        var existing = await masterRecurringSchedulesService.GetAsync(payload.Id);
        return existing is not null;
    }
    
    protected override TimeSpan DelayAfterProcessPayload() => 
        this.BackgroundAgentWorker.Mode == AgentWorkerMode.Drain ? TimeSpan.FromMilliseconds(50) : TimeSpan.FromMilliseconds(250);
    
    protected override TimeSpan LongDelayAfterBufferSize() => 
        this.BackgroundAgentWorker.Mode == AgentWorkerMode.Drain ? TimeSpan.FromMilliseconds(250) : TimeSpan.FromMilliseconds(1000);
}
