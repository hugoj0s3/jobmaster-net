using JobMaster.Abstractions.Models;

namespace JobMaster.Abstractions.Ioc.Selectors;

public interface IClusterConfigSelector
{
    public IClusterConfigSelector SetAsDefault();
    public IClusterConfigSelector ClusterId(string clusterId);

    public IClusterConfigSelector ClusterDefaultJobTimeout(TimeSpan defaultJobTimeout);

    public IClusterConfigSelector ClusterTransientThreshold(TimeSpan transientThreshold);
    public IClusterConfigSelector ClusterDefaultMaxRetryCount(int defaultMaxRetryCount);
    public IClusterConfigSelector ClusterMaxMessageByteSize(int maxMessageByteSize);
    public IClusterConfigSelector ClusterIanaTimeZoneId(string ianaTimeZoneId);
    public IAgentWorkerSelector AddWorker(string? workerName = null, string? agentConnectionName = null, int batchSize = 250);
    
    public IClusterConfigSelector DebugJsonlFileLogger(string filePath, int maxBufferItems = 500, TimeSpan? flushInterval = null);
    
    public IAgentConnectionConfigSelector AddAgentConnectionConfig(
        string agentConnectionName,
        string? repoType = null,
        string? cnnString = null);

    public IClusterConfigSelector ClusterMode(ClusterMode mode);
}