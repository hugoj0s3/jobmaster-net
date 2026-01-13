using System;
using JobMaster.Sdk.Contracts;

namespace JobMaster.NatJetStreams;

internal class NatJetStreamConstants
{
    public const string RepositoryTypeId = "NatJetStream";
    public const int DefaultDbOperationThrottleLimitForAgent = 1000;
    
    public const string Prefix = $"jobmaster.";
    
    public const string HeaderCorrelationId = "jm-correlation-id";
    public const string HeaderReferenceTime = "jm-reference-time";
    public const string HeaderSignature = "jm-signature";
    public const string HeaderMessageId = "Nats-Msg-Id";
    public const string HeaderConcurrencyRisk = "jm-concurrency-risk";
    
    public const uint MaxMsgRetriesForLostRisk = 30;
    public const uint MaxMsgRetriesForNoLostRisk = 3;

    // Centralized timing configuration
    public static readonly TimeSpan ConsumerAckWait = JobMasterConstants.ClockSkewPadding + TimeSpan.FromSeconds(30);
    public static readonly TimeSpan MessageLockTtl = ConsumerAckWait + TimeSpan.FromHours(5);

    // Maximum threshold beyond which scheduled jobs should be held on master instead of onboarded
    public static readonly TimeSpan MaxThreshold = TimeSpan.FromMinutes(2);
    public static uint MaxDeliver => 1000;
}

