using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Abstractions.Ioc.Selectors;

public interface IClusterStandaloneConfigSelector
{
    public IClusterStandaloneConfigSelector SetAsDefault();
    public IClusterStandaloneConfigSelector ClusterId(string clusterId);
    public IClusterStandaloneConfigSelector ClusterDefaultJobTimeout(TimeSpan defaultJobTimeout);
    public IClusterStandaloneConfigSelector ClusterTransientThreshold(TimeSpan transientThreshold);
    public IClusterStandaloneConfigSelector ClusterDefaultMaxRetryCount(int defaultMaxRetryCount);
    public IClusterStandaloneConfigSelector ClusterMaxMessageByteSize(int maxMessageByteSize);
    public IClusterStandaloneConfigSelector ClusterIanaTimeZoneId(string ianaTimeZoneId);
    public IClusterStandaloneConfigSelector AddWorker(string? workerName = null, int batchSize = 250);
    public IClusterStandaloneConfigSelector ClusterMode(ClusterMode mode);
    
    internal IClusterStandaloneConfigSelector ClusterConnString(string connString);
    internal IClusterStandaloneConfigSelector ClusterRepoType(string repoType);
    
    internal IClusterStandaloneConfigSelector ClusterAdditionalConfig(JobMasterConfigDictionary additionalConfig);
}