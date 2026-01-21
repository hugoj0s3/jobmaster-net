using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;

namespace JobMaster.Sdk.Abstractions.Ioc.Selectors;

public interface IAgentConnectionConfigSelectorAdvanced : IAgentConnectionConfigSelector
{
    public IAgentConnectionConfigSelector AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig);
    
    public IAgentConnectionConfigSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);
    
    public IAgentConnectionConfigSelector AgentRepoType(string repoType);
    public IAgentConnectionConfigSelector AgentConnString(string connString);
    public IAgentConnectionConfigSelector RuntimeDbOperationThrottleLimit(int limit);
  
}