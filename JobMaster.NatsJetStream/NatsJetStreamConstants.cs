using System;
using JobMaster.Sdk.Abstractions;

namespace JobMaster.NatsJetStream;

internal class NatsJetStreamConstants
{
    public const string RepositoryTypeId = "NatsJetStream";
    public const int DefaultDbOperationThrottleLimitForAgent = 1000;
    
    public const string Prefix = $"jobmaster.";
    
    public const string HeaderCorrelationId = "jm-correlation-id";
    public const string HeaderReferenceTime = "jm-reference-time";
    public const string HeaderSignature = "jm-signature";
    public const string HeaderMessageId = "Nats-Msg-Id";
    public const string HeaderHeartbeat = "jm-heartbeat";
    public const string HeaderConcurrencyRisk = "jm-concurrency-risk";
    
    public const uint MaxMsgRetriesForLostRisk = 30;
    public const uint MaxMsgRetriesForNoLostRisk = 3;

    // Centralized timing configuration
    public static readonly TimeSpan MinConsumerAckWait = JobMasterConstants.ClockSkewPadding + TimeSpan.FromSeconds(30);
    public static readonly TimeSpan AckOperationTimeout = TimeSpan.FromSeconds(5);
    
    public const int MinMaxAckPending = 100;

    public static int CalcMaxAckPending(int bucketBufferSize)
    {
        var maxAckPending = bucketBufferSize - MinMaxAckPending;
        return Math.Max(maxAckPending, MinMaxAckPending);
    }

    public static TimeSpan CalcAckWait(TimeSpan bucketBufferLeadTime) =>
        bucketBufferLeadTime + MinConsumerAckWait;

    public static TimeSpan CalcAckProgressKeepAliveInterval(TimeSpan bucketBufferLeadTime) =>
        TimeSpan.FromTicks(CalcAckWait(bucketBufferLeadTime).Ticks / 3);
    
    public static TimeSpan CalcMessageLockDuration(TimeSpan bucketBufferLeadTime) => 
        TimeSpan.FromMinutes(5) + NatsJetStreamConstants.CalcAckWait(bucketBufferLeadTime);

    // Maximum threshold beyond which scheduled jobs should be held on master instead of onboarded
    public static readonly TimeSpan MaxThreshold = TimeSpan.FromMinutes(2);
    public static uint MaxDeliver => 10000;

    // Backoff delays when onboarding is busy. Length also drives the max retry count.
    public static readonly TimeSpan[] BusyRetryDelays =
    [
        TimeSpan.FromSeconds(30),
        TimeSpan.FromSeconds(75),
        TimeSpan.FromMinutes(3)
    ];
}

