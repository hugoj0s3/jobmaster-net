namespace JobMaster.Sdk.Abstractions.Keys;

public class JobMasterInMemoryKeys : JobMasterKeyManager
{
    public JobMasterInMemoryKeys(string clusterId) : base("InMemoryCache", clusterId)
    {
    }
    
    public string BucketsAvailableForJobs() => CreateKey("BucketsAvailableForJobs");
    public string Bucket(string id) => CreateKey($"Bucket:{id}");
    
    public string AllAgentsWorkers() => CreateKey("AllAgentsWorkers");
    
    public string MasterConfiguration() => CreateKey("MasterConfiguration");
}