using JobMaster.Contracts.Ioc.Selectors;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models.Logs;

namespace JobMaster.Sdk.Contracts.Ioc.Selectors;

public interface IClusterConfigSelectorAdvanced : IClusterConfigSelector
{
    public IAgentConnectionConfigSelector AddAgentConnectionConfig(
        string agentConnectionName,
        string? repoType,
        string? cnnString,
        JobMasterConfigDictionary? additionalConnConfig);

    public IClusterConfigSelector ClusterAdditionalConfig(JobMasterConfigDictionary additionalConfig);

    public IClusterConfigSelector ClusterAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig);

    public IClusterConfigSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);
    public IClusterConfigSelector AppendAdditionalConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);
    
    public IClusterConfigSelector ClusterRuntimeDbOperationThrottleLimit(int runtimeDbOperationThrottleLimit);
    
    public IClusterConfigSelector ClusterRepoType(string repoType);
    public IClusterConfigSelector ClusterConnString(string connString);
    
    public IClusterConfigSelector EnableMirrorLog(Action<LogItem> mirrorLog);
    
    public void Finish();
}