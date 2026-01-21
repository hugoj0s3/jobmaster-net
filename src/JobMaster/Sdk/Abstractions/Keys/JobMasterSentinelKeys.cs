namespace JobMaster.Sdk.Abstractions.Keys;

public class JobMasterSentinelKeys : JobMasterKeyManager
{
    public JobMasterSentinelKeys(string clusterId) : base("Sentinel", clusterId)
    {
    }
    
    public string BucketsAvailableForJobs() => CreateKey("BucketsAvailableForJobs");

    public string GetMasterConfiguration() => CreateKey("MasterConfiguration");
    
    public string AgentsAndWorkers() => CreateKey("AgentsAndWorkers");

    public string Bucket(string id) => CreateKey($"Bucket:{id}");
}