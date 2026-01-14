using JobMaster.Contracts.Models;

namespace JobMaster.Contracts.Ioc.Selectors;

public interface IClusterConfigSelector
{
    public IClusterConfigSelector SetAsDefault();
    public IClusterConfigSelector ClusterId(string clusterId);
    public IClusterConfigSelector ClusterRepoType(string repoType);
    public IClusterConfigSelector ClusterConnString(string connString);

    public IClusterConfigSelector ClusterDefaultJobTimeout(TimeSpan defaultJobTimeout);

    public IClusterConfigSelector ClusterTransientThreshold(TimeSpan transientThreshold);
    public IClusterConfigSelector ClusterDefaultMaxRetryCount(int defaultMaxRetryCount);
    public IClusterConfigSelector ClusterMaxMessageByteSize(int maxMessageByteSize);
    public IClusterConfigSelector ClusterIanaTimeZoneId(string ianaTimeZoneId);
    public IClusterConfigSelector ClusterDbOperationThrottleLimit(int dbOperationThrottleLimit);
    public IClusterConfigSelector ClusterRuntimeDbOperationThrottleLimit(int runtimeDbOperationThrottleLimit);
    public IAgentWorkerSelector AddWorker(string? workerName = null, string? agentConnectionName = null, int batchSize = 250);
    
    public IAgentConnectionConfigSelector AddAgentConnectionConfig(
        string agentConnectionName,
        string? repoType = null,
        string? cnnString = null);

    IClusterConfigSelector ClusterMode(ClusterMode mode);
}