using System.Diagnostics;
using System.Text;
using System.Text.Json;
using JobMaster.Abstractions.Models;
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

    private bool hasInitialized;
    private Task? consumptionTask;
    private CancellationTokenSource? consumerCts;
    private int invalidBucketStatusTickCount = 0;
    private DateTime? taskCreatedAt = null;
    private DateTime? lastMessageReceivedAt = null;

    private int totalMessagesProcessed = 0;
    private TaskStatus? lastReportedTaskStatus = null;
    private DateTime lastHeartbeatPublishedAt = DateTime.MinValue;

    private AgentConnectionId agentConnectionId = null!;
    private INatsJSConsumer? consumer;
    
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> lockMessages = new();
    private readonly HashSet<(string MessageId, DateTime UnlockAt)> messagesToUnlock = new();

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
                
                if (invalidBucketStatusTickCount >= 6)
                {
                    logger.Info($"{GetRunnerDescription()}: Bucket {BucketId} status still invalid after {invalidBucketStatusTickCount} ticks, disposing CTS", JobMasterLogCategory.Bucket, BucketId);
                    consumerCts?.SafeDispose();
                    consumerCts = null;
                    invalidBucketStatusTickCount = 0;
                }
                else if (invalidBucketStatusTickCount >= 2)
                {
                    logger.Info($"{GetRunnerDescription()}: Bucket {BucketId} status is {bucket?.Status}, stopping consumer", JobMasterLogCategory.Bucket, BucketId);
                    consumerCts?.Cancel();
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
                BackgroundAgentWorker.BucketBufferSize,
                BackgroundAgentWorker.BucketBufferLeadTime,
                ct);
            hasInitialized = true;
        }

        // 3. Subscriber Startup & Watchdog
        if (consumptionTask == null)
        {
            logger.Info($"{GetRunnerDescription()}: Starting subscriber for bucket {BucketId}, fullBucketAddressId={fullBucketAddressId}", JobMasterLogCategory.Bucket, BucketId);
            consumerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var consumerToken = consumerCts.Token;
            
            // Publish initial heartbeat message to ensure consumer activates immediately
            await PublishHeartbeatAsync(fullBucketAddressId, ct);
            lastHeartbeatPublishedAt = DateTime.UtcNow;

            consumptionTask = Task.Run(async () => await ListenMsgsAsync(consumerToken, consumer!));

            taskCreatedAt = DateTime.UtcNow;
            lastReportedTaskStatus = null;
            totalMessagesProcessed = 0;
        }
        else if (IsTaskDead(consumptionTask))
        {
            logger.Warn($"{GetRunnerDescription()}: Subscriber for bucket {BucketId} is DEAD. Status={consumptionTask.Status}, IsFaulted={consumptionTask.IsFaulted}, IsCompleted={consumptionTask.IsCompleted}, IsCanceled={consumptionTask.IsCanceled}", JobMasterLogCategory.Bucket, BucketId);
            
            if (consumptionTask.IsFaulted && consumptionTask.Exception != null)
            {
                logger.Error($"{GetRunnerDescription()}: Task exception details for bucket {BucketId}", JobMasterLogCategory.Bucket, BucketId, consumptionTask.Exception);
            }
            
            consumptionTask?.SafeDispose();
            consumptionTask = null;
            consumerCts?.SafeDispose();
            consumerCts = null;
            await DoOnTickAsync(ct);
            return OnTickResult.Skipped(TimeSpan.FromSeconds(5));
        }
        else
        {
            var currentStatus = consumptionTask.Status;
            
            // Log status transitions (excluding WaitingForActivation as it's expected)
            if (lastReportedTaskStatus != currentStatus)
            {
                logger.Debug($"{GetRunnerDescription()}: Subscriber status changed for bucket {BucketId}. Status: {lastReportedTaskStatus} -> {currentStatus}, TotalMsgsProcessed={totalMessagesProcessed}", JobMasterLogCategory.Bucket, BucketId);
                lastReportedTaskStatus = currentStatus;
            }
        }

        // Heartbeat monitoring: publish at HeartbeatPublishInterval so ConsumeAsync always has a message
        // and never exits due to an empty stream. The 90s lost-detection uses lastMessageReceivedAt,
        // which is updated whenever the consumer processes any message (data or heartbeat).
        if (consumptionTask != null && !IsTaskDead(consumptionTask))
        {
            if (DateTime.UtcNow - lastHeartbeatPublishedAt > NatsJetStreamConstants.HeartbeatPublishInterval)
            {
                await PublishHeartbeatAsync(fullBucketAddressId, ct);
                lastHeartbeatPublishedAt = DateTime.UtcNow;
            }

            var timeSinceLastMessage = lastMessageReceivedAt.HasValue
                ? DateTime.UtcNow - lastMessageReceivedAt.Value
                : DateTime.UtcNow - taskCreatedAt!.Value;

            if (timeSinceLastMessage > TimeSpan.FromSeconds(90))
            {
                logger.Error($"{GetRunnerDescription()}: Consumer unresponsive for {timeSinceLastMessage.TotalSeconds:F0}s (no messages or heartbeats), marking bucket {BucketId} as lost", JobMasterLogCategory.Bucket, BucketId);
                await BackgroundAgentWorker.WorkerClusterOperations.MarkBucketAsLostAsync(BucketId!);
                await this.StopAsync();
                return OnTickResult.Failed(TimeSpan.FromMinutes(1));
            }
        }

        // Unlock messages whose redelivery retention window has expired
        lock (messagesToUnlock)
        {
            var now = DateTime.UtcNow;
            messagesToUnlock.RemoveWhere(x =>
            {
                if (x.UnlockAt > now) return false;
                lockMessages.TryRemove(x.MessageId, out _);
                return true;
            });
        }

        await DoOnTickAsync(ct);

        return OnTickResult.Success(this);
    }

    // ListenMsgsAsync loops ConsumeAsync internally and never exits normally, so RanToCompletion
    // should not occur in practice. Treat it as dead anyway so the watchdog can recover.
    private bool IsTaskDead(Task? t) => t != null && (t.IsFaulted || t.IsCanceled || t.Status == System.Threading.Tasks.TaskStatus.RanToCompletion);

    private async Task ListenMsgsAsync(CancellationToken ct, INatsJSConsumer consumer)
    {
        logger.Info($"{GetRunnerDescription()}: ListenMsgsAsync STARTED for bucket {BucketId}", JobMasterLogCategory.Bucket, BucketId);
        try
        {
            var opts = new NatsJSConsumeOpts
            {
                MaxMsgs =
                    (int)(NatsJetStreamConstants.CalcMaxAckPending(BackgroundAgentWorker.BucketBufferSize) * 0.75),
                IdleHeartbeat = NatsJetStreamConstants.ConsumerIdleHeartbeat,
            };

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await foreach (var msg in consumer.ConsumeAsync<byte[]>(opts: opts, cancellationToken: ct))
                    {
                        try
                        {
                            if (ct.IsCancellationRequested)
                            {
                                break;
                            }

                            // Update last message received timestamp for any message
                            lastMessageReceivedAt = DateTime.UtcNow;

                            // Send AckProgress to reset AckWait timer now that we're actually processing this message
                            // This prevents NATS from redelivering due to AckWait timeout while message sits in buffer
                            // Use an independent CTS — must not be tied to the consumer lifecycle token
                            using var progressCts = new CancellationTokenSource(NatsJetStreamConstants.AckOperationTimeout);
                            await msg.AckProgressAsync(cancellationToken: progressCts.Token).ConfigureAwait(false);

                            // Check if this is a heartbeat message and skip processing
                            var isHeartbeat = msg.Headers?.TryGetValue(NatsJetStreamConstants.HeaderHeartbeat, out _) == true;
                            if (isHeartbeat)
                            {
                                msg.Headers?.TryGetValue(NatsJetStreamConstants.HeaderSignature, out var signatureValue);
                                if (!IsSignatureValid(signatureValue))
                                {
                                    LogCriticalOrError($"{GetRunnerDescription()}: signature mismatch for heartbeat. Preview: Sig={signatureValue}");

                                    using var termCts = new CancellationTokenSource(NatsJetStreamConstants.AckOperationTimeout);
                                    await msg.AckTerminateAsync(cancellationToken: termCts.Token).ConfigureAwait(false);
                                    continue;
                                }

                                logger.Debug($"{GetRunnerDescription()}: Heartbeat message received for bucket {BucketId}", JobMasterLogCategory.Bucket, BucketId);
                                using var ackCts = new CancellationTokenSource(NatsJetStreamConstants.AckOperationTimeout);
                                await msg.AckAsync(cancellationToken: ackCts.Token).ConfigureAwait(false);
                                continue;
                            }

                            await ProcessMessageAsync(msg, ct).ConfigureAwait(false);
                        }
                        finally
                        {
                            Interlocked.Increment(ref totalMessagesProcessed);
                        }
                    }
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    // NATS internally cancelled the subscription (server-side close, heartbeat expiry, etc.)
                    // — NOT a user-requested stop. Fall through so the while loop restarts ConsumeAsync.
                    logger.Debug($"{GetRunnerDescription()} subscriber for bucket {BucketId} ConsumeAsync cancelled internally, restarting.", JobMasterLogCategory.Bucket, BucketId);
                }

                if (!ct.IsCancellationRequested)
                {
                    logger.Debug($"{GetRunnerDescription()} subscriber for bucket {BucketId} ConsumeAsync completed, restarting.", JobMasterLogCategory.Bucket, BucketId);
                }
            }
        }
        catch (OperationCanceledException)
        {
            this.logger.Info($"{GetRunnerDescription()} subscriber for bucket {BucketId} stopped.", JobMasterLogCategory.Bucket, BucketId);
        }
    }

    public void DefineBucketId(string bucketId)
    {
        BucketId = bucketId;
        var bucketModel = this.masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
        agentConnectionId = bucketModel!.AgentConnectionId;
    }

    protected virtual Task DoOnTickAsync(CancellationToken ct) => Task.CompletedTask;

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
                [NatsJetStreamConstants.HeaderMessageId] = JobMasterRandomUtil.NewGuid4().ToString(),
                [NatsJetStreamConstants.HeaderHeartbeat] = "true",
            };
            await jsContext!.PublishAsync(subjectName, data, headers: headers, cancellationToken: pubCts.Token);
            
            logger.Debug($"{GetRunnerDescription()}: Published heartbeat message for bucket {BucketId}", JobMasterLogCategory.Bucket, BucketId);
        }
        catch (Exception ex)
        {
            logger.Warn($"{GetRunnerDescription()}: Failed to publish heartbeat message for bucket {BucketId}: {ex.Message}", JobMasterLogCategory.Bucket, BucketId);
        }
    }


    private async Task ProcessMessageAsync(INatsJSMsg<byte[]> msg, CancellationToken ct)
    {
        var (signature, correlationId, referenceTimeUtc, messageId) = NatsJetStreamUtils.GetHeaderValues(msg.Headers);
        using var ackGuard = new MsgAckGuard(msg, messageId ?? JobMasterRandomUtil.NewGuid4().ToString());
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
                JobMasterLogCategory.Bucket,
                BucketId);

            await ackGuard.TryAckTerminateAsync().ConfigureAwait(false);
            return;
        }

        var json = Encoding.UTF8.GetString(msg.Data);
        if (!IsSignatureValid(signature))
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
        if (!lockMessages.TryAdd(messageId!, 0))
        {
            var shouldAck = await ShouldAckAfterLockAsync(payload, ct).ConfigureAwait(false);

            if (shouldAck)
            {
                await ackGuard.TryAckSuccessAsync().ConfigureAwait(false);
                logger.Debug($"{GetRunnerDescription()} acked-after-lock CorrId={correlationId} MessageId={messageId}", JobMasterLogCategory.Bucket, BucketId);
                return;
            }

            await ackGuard.TryNakAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            logger.Debug($"{GetRunnerDescription()} message already being processed, NAK'd. MessageId={messageId}", JobMasterLogCategory.Bucket, BucketId);
            return;
        }

        Stopwatch? sw = null;
        bool success = false;
        
        var keepAliveInterval = NatsJetStreamConstants.CalcAckProgressKeepAliveInterval(BackgroundAgentWorker.BucketBufferLeadTime);
        using var keepAliveCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var keepAliveTask = Task.Run(async () =>
        {
            while (!keepAliveCts.Token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(keepAliveInterval, keepAliveCts.Token);
                }
                catch (OperationCanceledException)
                {
                    break; // consumer stopping — exit cleanly
                }

                try
                {
                    await ackGuard.TryAckProgressAsync();
                }
                catch (Exception)
                {
                    // AckProgress failed — continue loop so next attempt still fires
                }
            }
        });

        try
        {
            logger.Debug($"{GetRunnerDescription()} processing-started CorrId={correlationId} MessageId={messageId} FailureAttempts={attempts}", JobMasterLogCategory.Bucket, BucketId);
            sw = Stopwatch.StartNew();

            logger.Debug(
                $"Processing {GetRunnerDescription()} payload {payload} CorrId={correlationId} RefTime={referenceTimeUtc} Sig={signature} MessageId={messageId} FailureAttempts={attempts}",
                JobMasterLogCategory.Bucket,
                BucketId);

            await ProcessPayloadAsync(payload, ackGuard).ConfigureAwait(false);

            await ackGuard.TryAckSuccessAsync().ConfigureAwait(false);
            success = true;
            logger.Debug($"{GetRunnerDescription()} acked CorrId={correlationId} MessageId={messageId}", JobMasterLogCategory.Bucket, BucketId);
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
                this.logger.Debug($"{GetRunnerDescription()}: ack-terminate (failureAttempts={ackGuard.FailureCount}) CorrId={corr} MsgId={mid}", JobMasterLogCategory.Bucket, BucketId);
                return;
            }

            this.logger.Error($"{GetRunnerDescription()}: failure (failureAttempts={ackGuard.FailureCount}). CorrId: {corr} RefTime: {rtime} Sig: {sig} MsgId: {mid}", JobMasterLogCategory.Bucket, BucketId, ex);

            await ackGuard.TryNakFailAsync().ConfigureAwait(false);
            this.logger.Debug($"{GetRunnerDescription()}: nak-fail requested (failureAttempts={ackGuard.FailureCount}) with delay CorrId={corr} MsgId={mid}", JobMasterLogCategory.Bucket, BucketId);
        }
        finally
        {
            // Stop keep-alive before unlock/logging — ensures no AckProgress fires after the final ack/nak
            keepAliveCts.Cancel();
            await keepAliveTask;

            if (ackGuard.Outcome == AckOutcome.Ack)
            {
                // Keep locked for 5 min + AckWait to cover any post-ACK redeliveries — OnTickAsync will clean up
                lock (messagesToUnlock)
                {
                    var messageLockDuration =
                        NatsJetStreamConstants.CalcMessageLockDuration(BackgroundAgentWorker.BucketBufferLeadTime);
                    messagesToUnlock.Add((messageId!, DateTime.UtcNow.Add(messageLockDuration)));
                }
            }
            else
            {
                // NAK/Terminate — remove immediately so redelivery is processed normally
                lockMessages.TryRemove(messageId!, out _);
            }

            if (sw != null)
            {
                sw.Stop();
                logger.Debug($"{GetRunnerDescription()} processing-duration CorrId={correlationId} MessageId={messageId} Success={success} ElapsedMs={sw.ElapsedMilliseconds}",
                    JobMasterLogCategory.Bucket, BucketId);
            }
        }
    }

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(1);

    public override async Task OnStopAsync()
    {
        await base.OnStopAsync();
        await StopConsumptionTaskAsync();
    }

    public override async Task OnTerminateFailureAsync(Exception lastException)
    {
        await base.OnTerminateFailureAsync(lastException);
        await StopConsumptionTaskAsync();
    }


    private static bool IsSignatureValid(string? signature) =>
        signature == NatsJetStreamConfigKey.NamespaceUniqueKey.ToString();

    protected void LogCriticalOrError(string message, Exception? ex = null)
    {
        if (LostRisk())
            this.logger.Critical(message, JobMasterLogCategory.Bucket, BucketId, exception: ex);
        else
            this.logger.Error(message, JobMasterLogCategory.Bucket, BucketId, exception: ex);
    }
    
    private async Task StopConsumptionTaskAsync()
    {
        this.logger.Info($"Stopping {GetRunnerDescription()} Runner for bucket {BucketId}. Waiting for subscriber task...", JobMasterLogCategory.Bucket, BucketId);

        consumerCts?.Cancel();

        if (consumptionTask != null)
        {
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(7));
            try
            {
                await consumptionTask.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
                this.logger.Info($"{GetRunnerDescription()} subscriber task stopped gracefully for bucket {BucketId}", JobMasterLogCategory.Bucket, BucketId);
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
            {
                this.logger.Warn($"{GetRunnerDescription()} subscriber task did not stop within timeout for bucket {BucketId}", JobMasterLogCategory.Bucket, BucketId);
            }
            catch (TimeoutException)
            {
                this.logger.Warn($"{GetRunnerDescription()} subscriber task did not stop within timeout for bucket {BucketId}", JobMasterLogCategory.Bucket, BucketId);
            }
            catch (Exception ex)
            {
                this.logger.Warn($"{GetRunnerDescription()} shutdown failed: {ex.Message}", JobMasterLogCategory.Bucket, BucketId, ex);
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

    // Hooks
    protected abstract string GetFullBucketAddressId(string bucketId);
    protected abstract bool LostRisk();
    protected abstract string GetRunnerDescription();
    protected abstract IReadOnlyCollection<BucketStatus> ValidBucketStatuses();
    protected abstract TPayload Deserialize(string json);
    protected abstract Task ProcessPayloadAsync(TPayload payload, MsgAckGuard ackGuard);
    protected abstract Task<bool> ShouldAckAfterLockAsync(TPayload payload, CancellationToken ct);
}
