using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingJobs;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Runners.SavePendingJobs;
using JobMaster.Sdk.Repositories;
using NATS.Client.JetStream;

namespace JobMaster.NatsJetStream.Background;

internal class NatsJetStreamDrainSavePendingJobsRunner : NatsJetStreamRunnerBase<JobRawModel>, IDrainSavePendingJobsRunner
{
    private readonly IMasterJobsService masterJobsService;
    private JobSavePendingOperation? savePendingOperation;
    
    public NatsJetStreamDrainSavePendingJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker, IMasterJobsService masterJobsService) : base(backgroundAgentWorker)
    {
        this.masterJobsService = masterJobsService;
    }


    protected override string GetFullBucketAddressId(string bucketId) => FullBucketAddressIdsUtil.GetJobSavePendingBucketAddress(bucketId);

    protected override bool LostRisk() => true;

    protected override string GetRunnerDescription() => "DrainSavePendingJob";

    protected override IReadOnlyCollection<BucketStatus> ValidBucketStatuses()
        => new[] { BucketStatus.Draining };

    protected override JobRawModel Deserialize(string json)
    {
        return InternalJobMasterSerializer.Deserialize<JobRawModel>(json);
    }

    protected override async Task ProcessPayloadAsync(JobRawModel payload, MsgAckGuard ackGuard)
    {
        if (savePendingOperation is null)
        {
            savePendingOperation = new JobSavePendingOperation(this.BackgroundAgentWorker, BucketId!);
        }
        
        var resultCode = await savePendingOperation.AddPendingSaveJobForDrainAsync(payload);
        if (resultCode == SaveDrainResultCode.Failed)
        {
            await ackGuard.TryNakFailAsync();
        }
    }

    protected override async Task<bool> ShouldAckAfterLockAsync(JobRawModel payload, CancellationToken ct)
    {
        var exists = await masterJobsService.GetAsync(payload.Id);
        return exists is not null;
    }
    
}