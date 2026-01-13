using System.Diagnostics;
using System.Text;
using System.Text.Json;
using JobMaster.Contracts.Extensions;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Background.Runners.DrainRunners;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Models.Agents;
using JobMaster.Sdk.Contracts.Models.Buckets;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Serialization;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Repositories;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Nito.AsyncEx;

namespace JobMaster.NatJetStreams.Background;

internal abstract class NatJetStreamRunnerBase<TPayload> : BucketAwareRunner
{
    protected readonly IMasterBucketsService masterBucketsService;
    private JobMaster.Sdk.Contracts.OperationThrottler ackThrottler = null!;

    private bool hasInitialized;
    private Task? consumptionTask;
    private CancellationTokenSource? consumerCts;
    private int invalidBucketStatusTickCount = 0;

    private int processCycleCount = 0;

    private AgentConnectionId agentConnectionId = null!;
    private INatsJSConsumer? consumer;
    
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> processingMessages = new();

    protected NatJetStreamRunnerBase(IJobMasterBackgroundAgentWorker backgroundAgentWorker)  : base(backgroundAgentWorker)
    {
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (string.IsNullOrEmpty(BucketId))
            return OnTickResult.Skipped(TimeSpan.FromSeconds(1));

        // 1. Bucket State Validation: Don't start if the bucket isn't in a processing state
        var bucket = masterBucketsService.Get(BucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket == null || !ValidBucketStatuses().Contains(bucket.Status))
        {
            // If consumer is already running, stop it since bucket status is no longer valid
            if (consumptionTask != null && !IsTaskDead(consumptionTask))
            {
                invalidBucketStatusTickCount++;
                
                if (invalidBucketStatusTickCount >= 2)
                {
                    logger.Info($"{GetRunnerDescription()}: Bucket {BucketId} status is {bucket?.Status}, stopping consumer", JobMasterLogSubjectType.Bucket, BucketId);
                    consumerCts?.Cancel();
                }
                else if (invalidBucketStatusTickCount >= 6)
                {
                    logger.Info($"{GetRunnerDescription()}: Bucket {BucketId} status still invalid after {invalidBucketStatusTickCount} ticks, disposing CTS", JobMasterLogSubjectType.Bucket, BucketId);
                    consumerCts?.SafeDispose();
                    consumerCts = null;
                    invalidBucketStatusTickCount = 0;
                }
            }
            return OnTickResult.Skipped(TimeSpan.FromSeconds(5));
        }
        
        // Reset counter when bucket status is valid
        invalidBucketStatusTickCount = 0;

        var fullBucketAddressId = GetFullBucketAddressId(BucketId!);

        // 2. Transport Initialization
        if (!hasInitialized)
        {
            NatJetStreamConnector.GetOrCreateConnection(this.BackgroundAgentWorker.JobMasterAgentConnectionConfig);
            await NatJetStreamConnector.EnsureStreamAsync(this.BackgroundAgentWorker.JobMasterAgentConnectionConfig);
            consumer = await NatJetStreamConnector.CreateOrUpdateConsumerAsync(
                this.BackgroundAgentWorker.JobMasterAgentConnectionConfig,
                fullBucketAddressId,
                BackgroundAgentWorker.BatchSize,
                ct);
            hasInitialized = true;
            ackThrottler =
                BackgroundAgentWorker
                    .Runtime!
                    .GetOperationThrottlerForAgent(this.BackgroundAgentWorker.JobMasterAgentConnectionConfig.ClusterId, this.BackgroundAgentWorker.JobMasterAgentConnectionConfig.Id);
        }

        // 4. Subscriber Startup & Watchdog
        if (consumptionTask == null)
        {
            logger.Info($"{GetRunnerDescription()}: Starting subscriber for bucket {BucketId}", JobMasterLogSubjectType.Bucket, BucketId);
            consumerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var consumerToken = consumerCts.Token;
            consumptionTask = Task.Run(async () => { await ListenMsgsAsync(consumerToken, consumer!); }, consumerToken);
        }
        else if (IsTaskDead(consumptionTask))
        {
            logger.Warn($"{GetRunnerDescription()}: Subscriber for bucket {BucketId} is DEAD. Status={consumptionTask.Status}, IsFaulted={consumptionTask.IsFaulted}, IsCompleted={consumptionTask.IsCompleted}, IsCanceled={consumptionTask.IsCanceled}", JobMasterLogSubjectType.Bucket, BucketId);
            
            if (consumptionTask.IsFaulted && consumptionTask.Exception != null)
            {
                logger.Error($"{GetRunnerDescription()}: Task exception details for bucket {BucketId}", JobMasterLogSubjectType.Bucket, BucketId, consumptionTask.Exception);
            }
            
            consumptionTask?.SafeDispose();
            consumptionTask = null;
            consumerCts?.SafeDispose();
            consumerCts = null;
            return OnTickResult.Skipped(TimeSpan.FromSeconds(5));
        }
        else
        {
            logger.Debug($"{GetRunnerDescription()}: Subscriber for bucket {BucketId} is running. Status={consumptionTask.Status}", JobMasterLogSubjectType.Bucket, BucketId);
        }

        await OnTickAfterSetupAsync(ct);

        return OnTickResult.Success(this);
    }

    // A subscriber task is considered "dead" only if it faulted or was canceled.
    // RanToCompletion means ConsumeAsync() finished normally (stream closed, no messages, etc.)
    // which is expected behavior and the task should be recreated.
    private bool IsTaskDead(Task? t) => t != null && (t.IsFaulted || t.IsCanceled || t.Status == System.Threading.Tasks.TaskStatus.RanToCompletion);

    private async Task ListenMsgsAsync(CancellationToken ct, INatsJSConsumer consumer)
    {
        try
        {
            await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: ct))
            {
                try
                {
                    if (ct.IsCancellationRequested)
                    {
                        break;
                    }

                    await ProcessMessageAsync(msg, ct).ConfigureAwait(false);
                    var successDelay = DelayAfterProcessPayload();
                    if (successDelay > TimeSpan.Zero)
                    {
                        await Task.Delay(successDelay, ct).ConfigureAwait(false);
                    }
                }
                finally
                {
                    Interlocked.Increment(ref processCycleCount);
                    if (processCycleCount >= BackgroundAgentWorker.BatchSize)
                    {
                        Interlocked.Exchange(ref processCycleCount, 0);
                        await Task.Delay(LongDelayAfterBatchSize(), ct).ConfigureAwait(false);
                    }

                    var jitter = JobMasterRandomUtil.GetInt(0, 50);
                    await Task.Delay(jitter, ct).ConfigureAwait(false);
                }
            }
            
            logger.Info($"{GetRunnerDescription()} subscriber for bucket {BucketId} stop to reading msg.", JobMasterLogSubjectType.Bucket, BucketId);
        }
        catch (OperationCanceledException)
        {
            this.logger.Info($"{GetRunnerDescription()} subscriber for bucket {BucketId} stopped.", JobMasterLogSubjectType.Bucket, BucketId);
        }
    }

    public void DefineBucketId(string bucketId)
    {
        BucketId = bucketId;
        var bucketModel = this.masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
        agentConnectionId = bucketModel!.AgentConnectionId;
    }

    protected virtual TimeSpan DelayAfterProcessPayload() => TimeSpan.FromMilliseconds(250);

    protected virtual TimeSpan DelayAfterProcessPayloadFails() => TimeSpan.FromSeconds(2);

    protected virtual TimeSpan LongDelayAfterBatchSize() => TimeSpan.FromSeconds(2.5);

    protected virtual Task OnTickAfterSetupAsync(CancellationToken ct) => Task.CompletedTask;


    private async Task ProcessMessageAsync(INatsJSMsg<byte[]> msg, CancellationToken ct)
    {
        var (signature, correlationId, referenceTimeUtc, messageId) = NatJetStreamUtils.GetHeaderValues(msg.Headers);
        var ackGuard = new MsgAckGuard(msg, messageId ?? Guid.NewGuid().ToString());
        var attempts = ackGuard.FailureCount;
        var natsDeliveryCount = msg.Metadata?.NumDelivered ?? 0;
        
        // Safety check: NATS delivery count exceeds MaxAckPending threshold
        if (natsDeliveryCount > NatJetStreamConstants.MaxDeliver - 1)
        {
            var preview = msg.Data != null ? NatJetStreamUtils.LogPreview(Encoding.UTF8.GetString(msg.Data), 128) : "null";
            LogCriticalOrError($"{GetRunnerDescription()}: NATS delivery count exceeded MaxAckPending threshold. NumDelivered={natsDeliveryCount} Preview: {preview} CorrId={correlationId} MsgId={messageId}");
            await ackGuard.TryAckTerminateAsync().ConfigureAwait(false);
            return;
        }
        
        if (msg.Data is null)
        {
            this.logger.Error(
                $"{GetRunnerDescription()}: msg data null. CorrId={correlationId} RefTime={referenceTimeUtc} Sig={signature} MsgId={messageId}",
                JobMasterLogSubjectType.Bucket,
                BucketId);

            await ackGuard.TryAckTerminateAsync().ConfigureAwait(false);
            return;
        }

        var json = Encoding.UTF8.GetString(msg.Data);
        if (signature is null || signature != NatJetStreamConfigKey.NamespaceUniqueKey.ToString())
        {
            var preview = NatJetStreamUtils.LogPreview(json, 128);
            LogCriticalOrError($"{GetRunnerDescription()}: signature mismatch. Preview: {preview} CorrId={correlationId} RefTime={referenceTimeUtc} Sig={signature} MsgId={messageId}");

            await ackGuard.TryAckTerminateAsync().ConfigureAwait(false);
            return;
        }

        TPayload payload;
        try
        {
            payload = Deserialize(json);
        }
        catch (JsonException jex)
        {
            var preview = NatJetStreamUtils.LogPreview(json, 128);
            LogCriticalOrError($"{GetRunnerDescription()}: malformed JSON. Preview: {preview} MsgId={messageId}", jex);

            await ackGuard.TryAckTerminateAsync().ConfigureAwait(false);
            return;
        }

        // In-memory duplicate detection: if already processing this message, NAK it
        if (!processingMessages.TryAdd(messageId!, 0))
        {
            bool shouldAck = false;
            shouldAck = await ShouldAckAfterLockAsync(payload, ct).ConfigureAwait(false);
            
            if (shouldAck)
            {
                await ackGuard.TryAckSuccessAsync(messageId!).ConfigureAwait(false);
                logger.Debug($"{GetRunnerDescription()} acked-after-lock CorrId={correlationId} MessageId={messageId}", JobMasterLogSubjectType.Bucket, BucketId);
                return;
            }
            
            await ackGuard.TryNakAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            logger.Debug($"{GetRunnerDescription()} message already being processed, NAK'd. MessageId={messageId}", JobMasterLogSubjectType.Bucket, BucketId);
            return;
        }

        Stopwatch? sw = null;
        bool success = false;
        try
        {
            logger.Debug($"{GetRunnerDescription()} processing-started CorrId={correlationId} MessageId={messageId} FailureAttempts={attempts}", JobMasterLogSubjectType.Bucket, BucketId);
            sw = Stopwatch.StartNew();

            logger.Debug(
                $"Processing {GetRunnerDescription()} payload {payload} CorrId={correlationId} RefTime={referenceTimeUtc} Sig={signature} MessageId={messageId} FailureAttempts={attempts}",
                JobMasterLogSubjectType.Bucket,
                BucketId);

            await ProcessPayloadAsync(payload, ackGuard).ConfigureAwait(false);

            await ackGuard.TryAckSuccessAsync(messageId!).ConfigureAwait(false);
            success = true;
            logger.Debug($"{GetRunnerDescription()} acked CorrId={correlationId} MessageId={messageId}", JobMasterLogSubjectType.Bucket, BucketId);
        }
        catch (Exception ex)
        {
            
            var (sig, corr, rtime, mid) = NatJetStreamUtils.GetHeaderValues(msg.Headers);
            ulong maxRetries = LostRisk() ? NatJetStreamConstants.MaxMsgRetriesForLostRisk : NatJetStreamConstants.MaxMsgRetriesForNoLostRisk;
            
            // Check current failure count before incrementing
            if (ackGuard.FailureCount >= maxRetries)
            {
                var preview = NatJetStreamUtils.LogPreview(Encoding.UTF8.GetString(msg.Data ?? Array.Empty<byte>()), 128);
                LogCriticalOrError($"{GetRunnerDescription()}: exhausted retries. Preview: {preview} CorrId: {corr} RefTime: {rtime} Sig: {sig} MsgId: {mid}", ex);
                await ackGuard.TryAckTerminateAsync().ConfigureAwait(false);
                this.logger.Debug($"{GetRunnerDescription()}: ack-terminate (failureAttempts={ackGuard.FailureCount}) CorrId={corr} MsgId={mid}", JobMasterLogSubjectType.Bucket, BucketId);
                return;
            }

            this.logger.Error($"{GetRunnerDescription()}: failure (failureAttempts={ackGuard.FailureCount}). CorrId: {corr} RefTime: {rtime} Sig: {sig} MsgId: {mid}", JobMasterLogSubjectType.Bucket, BucketId, ex);

            await ackGuard.TryNakFailAsync(messageId!).ConfigureAwait(false);
            this.logger.Debug($"{GetRunnerDescription()}: nak-fail requested (failureAttempts={ackGuard.FailureCount}) with delay CorrId={corr} MsgId={mid}", JobMasterLogSubjectType.Bucket, BucketId);
        }
        finally
        {
            // Remove from processing tracking
            processingMessages.TryRemove(messageId!, out _);
            
            if (sw != null)
            {
                sw.Stop();
                logger.Debug($"{GetRunnerDescription()} processing-duration CorrId={correlationId} MessageId={messageId} Success={success} ElapsedMs={sw.ElapsedMilliseconds}",
                    JobMasterLogSubjectType.Bucket, BucketId);
            }
        }
    }

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(10);

    public override async Task OnStopAsync()
    {
        await base.OnStopAsync();
        this.logger.Info($"Stopping {GetRunnerDescription()} Runner for bucket {BucketId}. Waiting for subscriber task...", JobMasterLogSubjectType.Bucket, BucketId);

        if (consumptionTask != null)
        {
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(7));
            try
            {
                await consumptionTask.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
                this.logger.Info($"{GetRunnerDescription()} subscriber task stopped gracefully for bucket {BucketId}", JobMasterLogSubjectType.Bucket, BucketId);
            }
            catch (TimeoutException)
            {
                this.logger.Warn($"{GetRunnerDescription()} subscriber task did not stop within timeout for bucket {BucketId}", JobMasterLogSubjectType.Bucket, BucketId);
            }
            catch (Exception ex)
            {
                this.logger.Warn($"{GetRunnerDescription()} shutdown failed: {ex.Message}", JobMasterLogSubjectType.Bucket, BucketId, ex);
            }
            finally
            {
                consumptionTask?.SafeDispose();
                consumptionTask = null;
                consumerCts?.SafeDispose();
                consumerCts = null;
            }
        }

        hasInitialized = false;
    }


    protected void LogCriticalOrError(string message, Exception? ex = null)
    {
        if (LostRisk())
            this.logger.Critical(message, JobMasterLogSubjectType.Bucket, BucketId, exception: ex);
        else
            this.logger.Error(message, JobMasterLogSubjectType.Bucket, BucketId, exception: ex);
    }

    // Hooks
    protected abstract string GetFullBucketAddressId(string bucketId);
    protected abstract bool LostRisk();
    protected abstract string GetRunnerDescription();
    protected abstract IReadOnlyCollection<BucketStatus> ValidBucketStatuses();
    protected abstract TPayload Deserialize(string json);
    protected abstract Task ProcessPayloadAsync(TPayload payload, MsgAckGuard ackGuard);
    protected abstract Task<bool> ShouldAckAfterLockAsync(TPayload payload, CancellationToken ct);
}