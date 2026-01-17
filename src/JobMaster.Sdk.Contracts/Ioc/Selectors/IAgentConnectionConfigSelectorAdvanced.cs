using JobMaster.Contracts.Ioc.Selectors;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models.Logs;

namespace JobMaster.Sdk.Contracts.Ioc.Selectors;

public interface IAgentConnectionConfigSelectorAdvanced : IAgentConnectionConfigSelector
{
    public IAgentConnectionConfigSelector AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig);
    
    public IAgentConnectionConfigSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);
    
    public IAgentConnectionConfigSelector AgentRepoType(string repoType);
    public IAgentConnectionConfigSelector AgentConnString(string connString);
    public IAgentConnectionConfigSelector RuntimeDbOperationThrottleLimit(int limit);
  
}