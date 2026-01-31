using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;

namespace JobMaster.Abstractions.Ioc.Selectors;

public interface IAgentConnectionConfigSelector
{
    
    public IAgentConnectionConfigSelector AgentConnName(string agentConnName);
    
    internal IAgentConnectionConfigSelector AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig);
    
    internal IAgentConnectionConfigSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);
    
    internal IAgentConnectionConfigSelector AgentRepoType(string repoType);
    internal IAgentConnectionConfigSelector AgentConnString(string connString);
    internal IAgentConnectionConfigSelector RuntimeDbOperationThrottleLimit(int limit);
}