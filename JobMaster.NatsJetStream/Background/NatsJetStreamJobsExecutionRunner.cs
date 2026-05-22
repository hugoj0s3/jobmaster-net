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
    private volatile TaskCompletionSource<bool> pulseSignal = new();
    
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
        // Wait for the next pulse cycle to fully complete before processing — ensures flush has run and
        // slots may have been freed. Triggered when the engine is full, or randomly (5%) as light backpressure.
        if (jobsExecutionEngine?.HasOnBoardingAvailability() == false || JobMasterRandomUtil.GetBoolean(0.05))
            await Task.WhenAny(pulseSignal.Task, Task.Delay(TimeSpan.FromMilliseconds(1500)));

        await DoProcessPayloadAsync(payload, ackGuard);
    }

    private async Task DoProcessPayloadAsync(JobRawModel payload, MsgAckGuard ackGuard)
    {
        logger.Debug($"Processing payload: JobId={payload.Id}");
        
        var utcNow = DateTime.UtcNow;
        if (jobsExecutionEngine is null)
        {
            jobsExecutionEngine = this.BackgroundAgentWorker.GetOrCreateEngine(this.Priority, this.BucketId!);
        }
        
        var onBoardingResult = await jobsExecutionEngine.TryOnBoardingJobAsync(payload);

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
            return;
        }

        if (onBoardingResult == OnBoardingResult.MovedToMaster)
        {
            logger.Debug($"{GetRunnerDescription()}: MovedToMaster. JobId={payload.Id}", JobMasterLogCategory.Job, payload.Id);
            return;
        }

        if (onBoardingResult == OnBoardingResult.Invalid)
        {
            logger.Warn($"{GetRunnerDescription()}: Invalid onboarding result. JobId={payload.Id} Status={payload.Status}", JobMasterLogCategory.Job, payload.Id);
            payload.MarkAsHeldOnMaster();
            await this.BackgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(payload));
            return;
        }

        if (onBoardingResult == OnBoardingResult.Busy)
        {
            var busyResult = await ackGuard.TryNakBusyAsync();

            if (busyResult == NackBusyResult.RetriesExhausted)
            {
                payload.MarkAsHeldOnMaster();
                await this.BackgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(payload));
                logger.Warn($"{GetRunnerDescription()}: Onboarding busy, retries exhausted. Moved to master. JobId={payload.Id}", JobMasterLogCategory.Job, payload.Id);
            }
            else
            {
                logger.Debug($"{GetRunnerDescription()}: Onboarding busy, NAK'd for retry. JobId={payload.Id}", JobMasterLogCategory.Job, payload.Id);
            }

            return;
        }
    }

    protected override async Task DoOnTickAsync(CancellationToken ct)
    {
        if (jobsExecutionEngine is null) return;
        
        await jobsExecutionEngine.PulseAsync();
        
        var prev = Interlocked.Exchange(ref pulseSignal, new TaskCompletionSource<bool>());
        prev.TrySetResult(true);
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

    public override TimeSpan WarmUpInterval => TimeSpan.FromMilliseconds(125);
    
    public override TimeSpan SucceedInterval => TimeSpan.FromMilliseconds(250);

}