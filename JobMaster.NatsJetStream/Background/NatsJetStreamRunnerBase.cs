using System.Diagnostics;
using System.Text;
using System.Text.Json;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Utils.Extensions;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Background.Runners.DrainRunners;
using JobMaster.Sdk.Repositories;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Nito.AsyncEx;

namespace JobMaster.NatsJetStream.Background;

internal abstract class NatsJetStreamRunnerBase<TPayload> : BucketAwareRunner
{
    protected readonly IMasterBucketsService masterBucketsService;
    private OperationThrottler ackThrottler = null!;

    private bool hasInitialized;
    private Task? consumptionTask;
    private CancellationTokenSource? consumerCts;
    private int invalidBucketStatusTickCount = 0;
    private DateTime? taskCreatedAt = null;
    private DateTime? lastMessageReceivedAt = null;

    private int processCycleCount = 0;
    private int totalMessagesProcessed = 0;
    private TaskStatus? lastReportedTaskStatus = null;

    private AgentConnectionId agentConnectionId = null!;
    private INatsJSConsumer? consumer;
    
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> processingMessages = new();

    protected NatsJetStreamRunnerBase(IJobMasterBackgroundAgentWorker backgroundAgentWorker)  : base(backgroundAgentWorker)
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
            NatsJetStreamConnector.GetOrCreateConnection(this.BackgroundAgentWorker.JobMasterAgentConnectionConfig);
            await NatsJetStreamConnector.EnsureStreamAsync(this.BackgroundAgentWorker.JobMasterAgentConnectionConfig);
            consumer = await NatsJetStreamConnector.CreateOrUpdateConsumerAsync(
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
            logger.Info($"{GetRunnerDescription()}: Starting subscriber for bucket {BucketId}, fullBucketAddressId={fullBucketAddressId}", JobMasterLogSubjectType.Bucket, BucketId);
            consumerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var consumerToken = consumerCts.Token;
            
            // Publish initial heartbeat message to ensure consumer activates immediately
            await PublishHeartbeatAsync(fullBucketAddressId, ct);
            
            consumptionTask = Task.Run(async () => await ListenMsgsAsync(consumerToken, consumer!), consumerToken);
            
            taskCreatedAt = DateTime.UtcNow;
            lastReportedTaskStatus = null;
            totalMessagesProcessed = 0;
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
            var currentStatus = consumptionTask.Status;
            
            // Log status transitions (excluding WaitingForActivation as it's expected)
            if (lastReportedTaskStatus != currentStatus)
            {
                logger.Debug($"{GetRunnerDescription()}: Subscriber status changed for bucket {BucketId}. Status: {lastReportedTaskStatus} -> {currentStatus}, TotalMsgsProcessed={totalMessagesProcessed}", JobMasterLogSubjectType.Bucket, BucketId);
                lastReportedTaskStatus = currentStatus;
            }
        }

        // Heartbeat monitoring: publish heartbeat if no message received recently
        if (consumptionTask != null && !IsTaskDead(consumptionTask))
        {
            var timeSinceLastMessage = lastMessageReceivedAt.HasValue 
                ? DateTime.UtcNow - lastMessageReceivedAt.Value 
                : DateTime.UtcNow - taskCreatedAt!.Value;
            
            // Publish heartbeat if no message received in last 10 seconds
            if (timeSinceLastMessage > TimeSpan.FromSeconds(10))
            {
                await PublishHeartbeatAsync(fullBucketAddressId, ct);
            }
            
            // Stop runner if no message received for 90 seconds
            if (timeSinceLastMessage > TimeSpan.FromSeconds(90))
            {
                logger.Error($"{GetRunnerDescription()}: No messages received for {timeSinceLastMessage.TotalSeconds:F0}s, stopping runner for bucket {BucketId}", JobMasterLogSubjectType.Bucket, BucketId);
                await BackgroundAgentWorker.WorkerClusterOperations.MarkBucketAsLostAsync(BucketId!);
                await this.StopAsync();
                return OnTickResult.Failed(TimeSpan.FromMinutes(1));
            }
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
        logger.Info($"{GetRunnerDescription()}: ListenMsgsAsync STARTED for bucket {BucketId}", JobMasterLogSubjectType.Bucket, BucketId);
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
                    
                    // Update last message received timestamp for any message
                    lastMessageReceivedAt = DateTime.UtcNow;
                    
                    
                    
                    // Check if this is a heartbeat message and skip processing
                    var isHeartbeat = msg.Headers?.TryGetValue(NatsJetStreamConstants.HeaderHeartbeat, out _) == true;
                    if (isHeartbeat)
                    {
                        var signatureIsTaken = msg.Headers?.TryGetValue(NatsJetStreamConstants.HeaderSignature, out var signatureValue);
                        
                        if ((signatureIsTaken == true && signatureValue != NatsJetStreamConfigKey.NamespaceUniqueKey.ToString()) || signatureIsTaken != true)
                        {
                            LogCriticalOrError($"{GetRunnerDescription()}: signature mismatch for heartbeat. Preview: Sig={signatureValue}");

                            await msg.AckTerminateAsync(cancellationToken: ct).ConfigureAwait(false);
                            return;
                        }
                        
                        logger.Debug($"{GetRunnerDescription()}: Heartbeat message received for bucket {BucketId}", JobMasterLogSubjectType.Bucket, BucketId);
                        await msg.AckAsync(cancellationToken: ct).ConfigureAwait(false);
                        continue;
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
                    Interlocked.Increment(ref totalMessagesProcessed);
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

    private async Task PublishHeartbeatAsync(string fullBucketAddressId, CancellationToken ct)
    {
        try
        {
            var (_, jsContext, _) = NatsJetStreamConnector.GetOrCreateConnection(this.BackgroundAgentWorker.JobMasterAgentConnectionConfig);
            var subjectName = NatsJetStreamUtils.GetSubjectName(agentConnectionId.IdValue, fullBucketAddressId);
            var data = Encoding.UTF8.GetBytes(string.Empty);
            
            using var pubCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var headers = new NatsHeaders
            {
                [NatsJetStreamConstants.HeaderSignature] = NatsJetStreamConfigKey.NamespaceUniqueKey.ToString(),
                [NatsJetStreamConstants.HeaderMessageId] = Guid.NewGuid().ToString(),
                [NatsJetStreamConstants.HeaderHeartbeat] = "true",
            };
            await jsContext!.PublishAsync(subjectName, data, headers: headers, cancellationToken: pubCts.Token);
            
            logger.Debug($"{GetRunnerDescription()}: Published heartbeat message for bucket {BucketId}", JobMasterLogSubjectType.Bucket, BucketId);
        }
        catch (Exception ex)
        {
            logger.Warn($"{GetRunnerDescription()}: Failed to publish heartbeat message for bucket {BucketId}: {ex.Message}", JobMasterLogSubjectType.Bucket, BucketId);
        }
    }


    private async Task ProcessMessageAsync(INatsJSMsg<byte[]> msg, CancellationToken ct)
    {
        var (signature, correlationId, referenceTimeUtc, messageId) = NatsJetStreamUtils.GetHeaderValues(msg.Headers);
        var ackGuard = new MsgAckGuard(msg, messageId ?? Guid.NewGuid().ToString());
        var attempts = ackGuard.FailureCount;
        var natsDeliveryCount = msg.Metadata?.NumDelivered ?? 0;
        
        // Safety check: NATS delivery count exceeds MaxAckPending threshold
        if (natsDeliveryCount > NatsJetStreamConstants.MaxDeliver - 1)
        {
            var preview = msg.Data != null ? NatsJetStreamUtils.LogPreview(Encoding.UTF8.GetString(msg.Data), 128) : "null";
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
        if (signature is null || signature != NatsJetStreamConfigKey.NamespaceUniqueKey.ToString())
        {
            var preview = NatsJetStreamUtils.LogPreview(json, 128);
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
            var preview = NatsJetStreamUtils.LogPreview(json, 128);
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
            
            var (sig, corr, rtime, mid) = NatsJetStreamUtils.GetHeaderValues(msg.Headers);
            ulong maxRetries = LostRisk() ? NatsJetStreamConstants.MaxMsgRetriesForLostRisk : NatsJetStreamConstants.MaxMsgRetriesForNoLostRisk;
            
            // Check current failure count before incrementing
            if (ackGuard.FailureCount >= maxRetries)
            {
                var preview = NatsJetStreamUtils.LogPreview(Encoding.UTF8.GetString(msg.Data ?? Array.Empty<byte>()), 128);
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