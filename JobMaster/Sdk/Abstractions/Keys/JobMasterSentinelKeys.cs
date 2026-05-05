namespace JobMaster.Sdk.Abstractions.Keys;

internal class JobMasterSentinelKeys : JobMasterKeyManager
{
    public JobMasterSentinelKeys(string clusterId) : base("Sentinel", clusterId)
    {
    }
    
    public string AllBuckets() => CreateKey("AllBuckets");

    public string GetMasterConfiguration() => CreateKey("MasterConfiguration");
    
    public string AgentsAndWorkers() => CreateKey("AgentsAndWorkers");

    public string Bucket(string id) => CreateKey($"Bucket:{id}");

    public string Hosts() => CreateKey("Hosts");

    public string AgentConnections() => CreateKey("AgentConnections");
}