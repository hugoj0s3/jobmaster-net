using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingJobs;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Runners.SavePendingJobs;
using JobMaster.Sdk.Repositories;
using NATS.Client.JetStream;

namespace JobMaster.NatsJetStream.Background;

internal class NatsJetStreamDrainProcessingRunner : NatsJetStreamRunnerBase<JobRawModel>, IDrainProcessingJobsRunner
{
    private IMasterDistributedLockerService masterDistributedLockerService;
    private JobMasterLockKeys jobmasterBaseLockKeys = null!;
    private readonly IMasterJobsService masterJobsService;
    private SavePendingOperation? savePendingOperation;

    public NatsJetStreamDrainProcessingRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        jobmasterBaseLockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
    }


    protected override string GetFullBucketAddressId(string bucketId) => FullBucketAddressIdsUtil.GetJobProcessingBucketAddress(bucketId);

    protected override bool LostRisk() => false;

    protected override string GetRunnerDescription() => "DrainProcessingJob";

    protected override IReadOnlyCollection<BucketStatus> ValidBucketStatuses() => new[] { BucketStatus.Draining };

    protected override JobRawModel Deserialize(string json) => InternalJobMasterSerializer.Deserialize<JobRawModel>(json);

    protected override async Task ProcessPayloadAsync(JobRawModel job, MsgAckGuard ackGuard)
    {
        if (savePendingOperation is null)
        {
            savePendingOperation = new SavePendingOperation(this.BackgroundAgentWorker, this.BucketId!);
        }
        
        await savePendingOperation.SaveDrainProcessingAsync(job);
    }

    protected override async Task<bool> ShouldAckAfterLockAsync(JobRawModel payload, CancellationToken ct)
    {
        var exists = await masterJobsService.GetAsync(payload.Id);
        return exists is not null;
    }

    protected override TimeSpan DelayAfterProcessPayload() => 
        this.BackgroundAgentWorker.Mode == AgentWorkerMode.Drain ? TimeSpan.FromMilliseconds(50) : TimeSpan.FromMilliseconds(250);
    
    protected override TimeSpan LongDelayAfterBatchSize() => 
        this.BackgroundAgentWorker.Mode == AgentWorkerMode.Drain ? TimeSpan.FromMilliseconds(250) : TimeSpan.FromMilliseconds(1000);
}