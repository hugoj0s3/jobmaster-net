using System;
using JobMaster.Sdk.Abstractions;

namespace JobMaster.NatsJetStream;

internal class NatsJetStreamConstants
{
    public const string RepositoryTypeId = "NatsJetStream";
    
    public const string Prefix = $"jobmaster.";
    
    public const string HeaderCorrelationId = "jm-correlation-id";
    public const string HeaderReferenceTime = "jm-reference-time";
    public const string HeaderSignature = "jm-signature";
    public const string HeaderMessageId = "Nats-Msg-Id";
    public const string HeaderHeartbeat = "jm-heartbeat";
    public const string HeaderConcurrencyRisk = "jm-concurrency-risk";
    
    public const uint MaxMsgRetriesForLostRisk = 50;
    public const uint MaxMsgRetriesForNoLostRisk = 5;

    // Centralized timing configuration
    public static readonly TimeSpan AckWaitSafetyMargin = JobMasterConstants.ClockSkewPadding + TimeSpan.FromSeconds(30);
    public static readonly TimeSpan AckOperationTimeout = TimeSpan.FromSeconds(5);
    
    public const int MinMaxAckPending = 100;

    // Fetch/prefetch batch size for ConsumeAsync — half of bucketBufferSize, floored at MinMaxAckPending
    // so small buffer configs don't end up with a degenerate near-empty batch. Deliberately independent
    // of TransientThreshold/CalcMaxAckPending: prefetch size is about active-processing capacity, not
    // how long parked (TooEarly) messages get held.
    public static int CalcMaxMsgs(int bucketBufferSize) =>
        (int)(Math.Max(MinMaxAckPending, bucketBufferSize) * 0.5);

    // Scales with TransientThreshold so the sustained-throughput ceiling (MaxAckPending / hold-duration)
    // stays roughly constant regardless of how long TooEarly jobs get parked via Nak. Each 30s of
    // TransientThreshold contributes 1 unit of multiplier, floored at 2 (i.e. TransientThreshold <= 1min)
    // so short/misconfigured thresholds never shrink pending capacity below bucketBufferSize * 2 (a floor
    // of exactly bucketBufferSize would leave zero room for parked messages alongside a full fetch batch),
    // and floored again at MinMaxAckPending as an absolute minimum for small bucketBufferSize configs.
    public static int CalcMaxAckPending(int bucketBufferSize, TimeSpan transientThreshold)
    {
        var thresholdUnits = Math.Max(transientThreshold.TotalSeconds / 30.0, 2);
        var result = (int)(bucketBufferSize * thresholdUnits);
        return Math.Max(result, MinMaxAckPending);
    }

    public static TimeSpan CalcAckWait(TimeSpan bucketBufferLeadTime) =>
        bucketBufferLeadTime + AckWaitSafetyMargin;

    public static TimeSpan CalcAckProgressKeepAliveInterval(TimeSpan bucketBufferLeadTime) =>
        TimeSpan.FromTicks(CalcAckWait(bucketBufferLeadTime).Ticks / 3);
    
    public static TimeSpan CalcMessageLockDuration(TimeSpan bucketBufferLeadTime) => 
        TimeSpan.FromMinutes(5) + NatsJetStreamConstants.CalcAckWait(bucketBufferLeadTime);

    // Maximum threshold beyond which scheduled jobs should be held on master instead of onboarded
    public static readonly TimeSpan MaxThreshold = TimeSpan.FromMinutes(5);
    public static uint MaxDeliver => 10000;

    // A publish whose Nats-Msg-Id was seen within this window is dropped. Each publish gets a new id, so it only
    // catches the same publish retried after a timeout. Same as the NATS default.
    public static readonly TimeSpan DuplicateWindow = TimeSpan.FromMinutes(2);

    // How long the stream keeps a message, acked or not (the stream's default is forever). Must outlast any outage:
    // a message not consumed by then is deleted.
    public static readonly TimeSpan MaxAge = TimeSpan.FromDays(30);

    // Copies of the stream across NATS cluster servers (NATS default). NATS itself rejects invalid values.
    public const int DefaultNumReplicas = 1;

    // Backoff delays when onboarding is busy. Length also drives the max retry count.
    public static readonly TimeSpan[] BusyRetryDelays =
    [
        TimeSpan.FromSeconds(30),
        TimeSpan.FromSeconds(75),
        TimeSpan.FromMinutes(3)
    ];

    // How often a jm-heartbeat message is published to each bucket's subject.
    // Keeps lastMessageReceivedAt fresh so idle buckets aren't marked Lost by the 90s unresponsive check.
    public static readonly TimeSpan HeartbeatPublishInterval = TimeSpan.FromSeconds(10);

    // NATS-level idle heartbeat sent by the server when no messages are available.
    // Keeps the pull subscription open during idle periods so ConsumeAsync doesn't need to restart.
    // Tuned independently of HeartbeatPublishInterval.
    public static readonly TimeSpan ConsumerIdleHeartbeat = TimeSpan.FromSeconds(5);
}

