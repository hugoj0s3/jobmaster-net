using System.Diagnostics;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Repositories;

namespace JobMaster.NatsJetStream.Background;

internal class NatsJetStreamJobsExecutionRunner : NatsJetStreamRunnerBase<JobRawModel>, IJobsExecutionRunner
{
    private IJobsExecutionEngine? jobsExecutionEngine;
    private readonly Stopwatch lifetimeSw = new();
    private readonly Dictionary<string, int> busyRetryCount = new();
    
    public NatsJetStreamJobsExecutionRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
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
        
        var utcNow = DateTime.UtcNow;
        if (jobsExecutionEngine is null)
        {
            jobsExecutionEngine = this.BackgroundAgentWorker.GetOrCreateEngine(this.Priority, this.BucketId!);
        }
        
        var onBoardingResult = await jobsExecutionEngine.TryOnBoardingJobAsync(payload);

        if (onBoardingResult != OnBoardingResult.Busy)
        {
            busyRetryCount.Remove(payload.Id.ToString());
        }

        if (onBoardingResult == OnBoardingResult.Cancelled)
        {
            // Job was cancelled (e.g., recurring schedule cancelled) - ACK to remove from queue
            logger.Debug($"{GetRunnerDescription()}: Job cancelled. JobId={payload.Id} Status={payload.Status}", JobMasterLogCategory.Job, payload.Id);
            return; // Message will be ACK'd automatically by NatsJetStreamRunnerBase
        }

        if (onBoardingResult == OnBoardingResult.TooEarly)
        {
            // Scheduling guard: avoid onboarding too early
            if (payload.GetSafeNextPlanExecutionAt() > utcNow + NatsJetStreamConstants.MaxThreshold.Add(JobMasterConstants.ClockSkewPadding))
            {
                payload.MarkAsHeldOnMaster();
                await this.BackgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(payload));
                logger.Warn($"{GetRunnerDescription()}: NextPlanExecutionAt > 2 minutes. HeldOnMaster and terminated. JobId={payload.Id} NextPlanExecutionAt={payload.NextPlanExecutionAt:O} now={utcNow:O}", JobMasterLogCategory.Job, payload.Id);
                return;
            }

            var target = payload.GetSafeNextPlanExecutionAt() - BackgroundAgentWorker.BucketBufferLeadTime;
            var delay = target > utcNow ? target - utcNow : TimeSpan.Zero;
            // Only add jitter when there is a real delay, to spread out redeliveries.
            // For immediate jobs (delay == Zero) jitter would just waste time.
            var jitter = delay > TimeSpan.Zero
                ? TimeSpan.FromMilliseconds(JobMasterRandomUtil.GetInt(5, 50))
                : TimeSpan.Zero;

            await ackGuard.TryNakAsync(delay + jitter);
            
            logger.Debug($"{GetRunnerDescription()}: NextPlanExecutionAt > {JobMasterConstants.OnBoardingWindow.TotalSeconds:F0}s ahead. Nak with delay={delay}. JobId={payload.Id} NextPlanExecutionAt={payload.NextPlanExecutionAt:O} now={utcNow:O}", JobMasterLogCategory.Job, payload.Id);
        }

        if (onBoardingResult == OnBoardingResult.Busy)
        {
            var jobId = payload.Id.ToString();
            busyRetryCount.TryGetValue(jobId, out var retryCount);
            retryCount++;

            if (retryCount >= NatsJetStreamConstants.BusyRetryDelays.Length)
            {
                busyRetryCount.Remove(jobId);
                payload.MarkAsHeldOnMaster();
                await this.BackgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(payload));
                logger.Warn($"{GetRunnerDescription()}: Onboarding busy after {retryCount} retries. Moved to master. JobId={payload.Id}", JobMasterLogCategory.Job, payload.Id);
                return;
            }

            busyRetryCount[jobId] = retryCount;

            var busyDelay = NatsJetStreamConstants.BusyRetryDelays[retryCount - 1];

            await ackGuard.TryNakAsync(busyDelay);
            logger.Debug($"{GetRunnerDescription()}: Onboarding busy, retry {retryCount}/3. Nak with delay={busyDelay}. JobId={payload.Id}", JobMasterLogCategory.Job, payload.Id);
            return;
        }
    }

    protected override async Task OnTickAfterSetupAsync(CancellationToken ct)
    {
        if (jobsExecutionEngine is null) return;
        
        await jobsExecutionEngine.PulseAsync();
    }

    protected override Task<bool> ShouldAckAfterLockAsync(JobRawModel payload, CancellationToken ct) => Task.FromResult(true);

    public void DefineBucketId(string bucketId, JobMasterPriority priority)
    {
        this.DefineBucketId(bucketId);
        this.Priority = priority;
        
        logger.Debug($"Bucket defined. bucketId {bucketId}, priority {priority}", JobMasterLogCategory.Bucket, bucketId);
    }
    
    public override async Task OnStopAsync()
    {
        // 1. Standard Backbone Shutdown (stops subscribers and waits for loops)
        await base.OnStopAsync();

        this.logger.Info($"{GetRunnerDescription()}: Starting graceful flush of buffered jobs for {BucketId}.", JobMasterLogCategory.Bucket, BucketId);

        if (jobsExecutionEngine is not null)
        {
            await jobsExecutionEngine.FlushToMasterAsync().ConfigureAwait(false);
        }
    }

    public JobMasterPriority Priority { get; internal set; }

    public override TimeSpan WarmUpInterval => TimeSpan.FromSeconds(1);
}