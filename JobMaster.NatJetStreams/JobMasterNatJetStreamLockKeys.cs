using JobMaster.Sdk.Contracts.Keys;

namespace JobMaster.NatJetStreams;

public class JobMasterNatJetStreamLockKeys : JobMasterKeyManager
{
    public JobMasterNatJetStreamLockKeys(string clusterId) : base("Lock", clusterId)
    {
    }

    protected override string BasePrefix() => NatJetStreamConfigKey.NamespaceUniqueKey.ToString();
    
    public string MessageLock(string messageId) => CreateKey($"MessageLock:{messageId}");
}