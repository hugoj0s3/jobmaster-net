using JobMaster.Sdk.Abstractions.Keys;

namespace JobMaster.NatsJetStream;

internal class JobMasterNatsJetStreamLockKeys : JobMasterKeyManager
{
    public JobMasterNatsJetStreamLockKeys(string clusterId) : base("Lock", clusterId)
    {
    }

    protected override string BasePrefix() => NatsJetStreamConfigKey.NamespaceUniqueKey.ToString();
    
    public string MessageLock(string messageId) => CreateKey($"MessageLock:{messageId}");
}