using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Logs;

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
    
    public IClusterStandaloneConfigSelector UseStandaloneCluster();
    
    internal IAgentConnectionConfigSelector AddAgentConnectionConfig(
        string agentConnectionName,
        string? repoType,
        string? cnnString,
        JobMasterConfigDictionary? additionalConnConfig);

    internal IClusterConfigSelector ClusterAdditionalConfig(JobMasterConfigDictionary additionalConfig);

    internal IClusterConfigSelector ClusterAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig);

    internal IClusterConfigSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);
    internal IClusterConfigSelector AppendAdditionalConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);
    
    internal IClusterConfigSelector ClusterRuntimeDbOperationThrottleLimit(int runtimeDbOperationThrottleLimit);
    
    internal IClusterConfigSelector ClusterRepoType(string repoType);
    internal IClusterConfigSelector ClusterConnString(string connString);
    
    internal IClusterConfigSelector EnableMirrorLog(Action<LogItem> mirrorLog);
    
    internal void Finish();
}