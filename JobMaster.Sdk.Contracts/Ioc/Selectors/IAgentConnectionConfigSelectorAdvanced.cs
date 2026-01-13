using JobMaster.Contracts.Ioc.Selectors;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Keys;

namespace JobMaster.Sdk.Contracts.Ioc.Selectors;

public interface IAgentConnectionConfigSelectorAdvanced : IAgentConnectionConfigSelector
{
    public IAgentConnectionConfigSelector AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig);
    
    public IAgentConnectionConfigSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);
    
    public IAgentConnectionConfigSelector AgentRuntimeDbOperationThrottleLimit(int runtimeDbOperationThrottleLimit);
}