using JobMaster.Sdk.Contracts.Keys;
using NATS.Client.Core;

namespace JobMaster.NatJetStreams;

internal static class NatJetStreamConfigKey
{
    internal static readonly JobMasterNamespaceUniqueKey NamespaceUniqueKey = 
        new("JobMasterNatJetStreams", "7ddc2917-8c17-4578-85e3-e76b7cb90d8f");
    
    internal static readonly string NatsAuthOptsKey = 
        "NatsAuthOpts";
    
    internal static readonly string NatsTlsOptsKey = 
        "NatsTlsOpts";
}