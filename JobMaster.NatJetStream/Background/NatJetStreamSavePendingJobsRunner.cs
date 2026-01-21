using System.Text;
using JobMaster.NatJetStream.Agents;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingJobs;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background.Runners.SavePendingJobs;
using JobMaster.Sdk.Repositories;
using JobMaster.Sdk.Services.Master;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace JobMaster.NatJetStream.Background;

internal sealed class NatJetStreamSavePendingJobsRunner : NatJetStreamRunnerBase<JobRawModel>, ISavePendingJobsRunner
{
    private readonly IMasterJobsService masterJobsService;
    private readonly IMasterClusterConfigurationService masterClusterConfigurationService;
    private SavePendingOperation? savePendingOperation;


    public NatJetStreamSavePendingJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        masterClusterConfigurationService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
    }

    protected override string GetRunnerDescription() => "SavePendingJob";

    protected override IReadOnlyCollection<BucketStatus> ValidBucketStatuses()
        => new[] { BucketStatus.Active, BucketStatus.Completing };

    protected override string GetFullBucketAddressId(string bucketId)
        => FullBucketAddressIdsUtil.GetJobSavePendingBucketAddress(bucketId);

    protected override bool LostRisk() => true;

    protected override JobRawModel Deserialize(string json)
        => InternalJobMasterSerializer.Deserialize<JobRawModel>(json);

    protected override async Task<bool> ShouldAckAfterLockAsync(JobRawModel payload, CancellationToken ct)
    {
        var exists = await masterJobsService.GetAsync(payload.Id);
        return exists is not null;
    }

    protected override async Task ProcessPayloadAsync(JobRawModel job, MsgAckGuard ackGuard)
    {
        if (savePendingOperation is null)
        {
            savePendingOperation = new SavePendingOperation(this.BackgroundAgentWorker, this.BucketId!);
        }
        
        var configuration = masterClusterConfigurationService.Get();
        var transientThreshold = configuration?.TransientThreshold ?? TimeSpan.FromMinutes(5);
        
        DateTime cutOffDate = DateTime.UtcNow.Add(transientThreshold);
        
        var result = await savePendingOperation.AddSavePendingJobAsync(job, cutOffDate);
        
        if (result.ResultCode == AddSavePendingResultCode.HeldOnMasterPublishedUnknown || 
            result.ResultCode == AddSavePendingResultCode.PublishFailed)
        {
            logger.Warn($"Failed {result.ResultCode}", JobMasterLogSubjectType.Job, job.Id, exception: result.Exception);
        }
    }
    
    protected override TimeSpan LongDelayAfterBatchSize() => TimeSpan.FromMilliseconds(100);
    protected override TimeSpan DelayAfterProcessPayload() => TimeSpan.Zero;
}