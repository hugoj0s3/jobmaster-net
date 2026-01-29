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
    public static readonly TimeSpan ConsumerAckWait = JobMasterConstants.ClockSkewPadding + TimeSpan.FromSeconds(30);

    // Maximum threshold beyond which scheduled jobs should be held on master instead of onboarded
    public static readonly TimeSpan MaxThreshold = TimeSpan.FromMinutes(2);
    public static uint MaxDeliver => 10000;
}

