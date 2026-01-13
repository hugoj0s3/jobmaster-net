using JobMaster.Contracts.Ioc.Selectors;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Keys;

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
    
    public void Finish();
}