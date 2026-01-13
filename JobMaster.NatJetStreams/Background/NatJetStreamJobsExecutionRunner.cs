using JobMaster.Contracts.Models;
using System.Diagnostics;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Background.Runners.JobsExecution;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Background.Runners;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Serialization;
using JobMaster.Sdk.Repositories;

namespace JobMaster.NatJetStreams.Background;

internal class NatJetStreamJobsExecutionRunner : NatJetStreamRunnerBase<JobRawModel>, IJobsExecutionRunner
{
    private JobsExecutionEngine? jobsExecutionOperation;
    private readonly Stopwatch lifetimeSw = new();
    
    public NatJetStreamJobsExecutionRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        lifetimeSw.Start();
    }

    protected override string GetFullBucketAddressId(string bucketId) => FullBucketAddressIdsUtil.GetJobProcessingBucketAddress(bucketId);

    protected override bool LostRisk() => false;

    protected override string GetRunnerDescription() => "JobExecution";

    protected override IReadOnlyCollection<BucketStatus> ValidBucketStatuses() => new[] { BucketStatus.Active, BucketStatus.Completing };

    protected override JobRawModel Deserialize(string json)
    {
        return InternalJobMasterSerializer.Deserialize<JobRawModel>(json);
    }

    protected override async Task ProcessPayloadAsync(JobRawModel payload, MsgAckGuard ackGuard)
    {
        logger.Debug($"Processing payload: JobId={payload.Id}");
        
        var now = DateTime.UtcNow;
        if (jobsExecutionOperation is null)
        {
            throw new InvalidOperationException("JobsExecutionOperation is not initialized.");
        }
        
        var onBoardingResult = await jobsExecutionOperation.TryOnBoardingJobAsync(payload);
        
        if (onBoardingResult == OnBoardingResult.Cancelled)
        {
            // Job was cancelled (e.g., recurring schedule cancelled) - ACK to remove from queue
            logger.Debug($"{GetRunnerDescription()}: Job cancelled. JobId={payload.Id} Status={payload.Status}", JobMasterLogSubjectType.Job, payload.Id);
            return; // Message will be ACK'd automatically by NatJetStreamRunnerBase
        }
        
        if (onBoardingResult == OnBoardingResult.TooEarly)
        {
            // Scheduling guard: avoid onboarding too early
            if (payload.ScheduledAt > now + NatJetStreamConstants.MaxThreshold.Add(JobMasterConstants.ClockSkewPadding))
            {
                payload.MarkAsHeldOnMaster();
                await this.BackgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(payload));
                logger.Warn($"{GetRunnerDescription()}: ScheduledAt > 2 minutes. HeldOnMaster and terminated. JobId={payload.Id} ScheduledAt={payload.ScheduledAt:O} now={now:O}", JobMasterLogSubjectType.Job, payload.Id);
                return;
            }
            
            var target = payload.ScheduledAt - JobMasterConstants.OnBoardingWindow;
            var delay = target > now ? target - now : TimeSpan.Zero;
            await ackGuard.TryNakAsync(delay);
            
            logger.Debug($"{GetRunnerDescription()}: ScheduledAt > {JobMasterConstants.OnBoardingWindow.TotalSeconds:F0}s ahead. Nak with delay={delay}. JobId={payload.Id} ScheduledAt={payload.ScheduledAt:O} now={now:O}", JobMasterLogSubjectType.Job, payload.Id);
        }
    }

    protected override async Task OnTickAfterSetupAsync(CancellationToken ct)
    {
        if (jobsExecutionOperation is null) return;
        
        await jobsExecutionOperation.PulseAsync();
    }

    protected override Task<bool> ShouldAckAfterLockAsync(JobRawModel payload, CancellationToken ct) => Task.FromResult(true);

    public void DefineBucketId(string bucketId, JobMasterPriority priority)
    {
        this.DefineBucketId(bucketId);
        this.Priority = priority;
        
        jobsExecutionOperation = new JobsExecutionEngine(this.BackgroundAgentWorker, this.BucketId!, this.Priority);
        
        logger.Debug($"Bucket defined. bucketId {bucketId}, priority {priority}", JobMasterLogSubjectType.Bucket, bucketId);
    }
    
    public override async Task OnStopAsync()
    {
        // 1. Standard Backbone Shutdown (stops subscribers and waits for loops)
        await base.OnStopAsync();

        this.logger.Info($"{GetRunnerDescription()}: Starting graceful flush of buffered jobs for {BucketId}.", JobMasterLogSubjectType.Bucket, BucketId);

        if (jobsExecutionOperation is not null)
        {
            await jobsExecutionOperation.FlushToMasterAsync().ConfigureAwait(false);
        }
    }

    public JobMasterPriority Priority { get; internal set; }

    protected override TimeSpan LongDelayAfterBatchSize() => TimeSpan.FromMilliseconds(10);
    protected override TimeSpan DelayAfterProcessPayload() => TimeSpan.Zero;
}