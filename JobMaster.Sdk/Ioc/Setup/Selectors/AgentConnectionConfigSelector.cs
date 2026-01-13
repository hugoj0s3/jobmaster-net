using JobMaster.Contracts.Ioc.Selectors;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Ioc.Definitions;
using JobMaster.Sdk.Contracts.Ioc.Selectors;
using JobMaster.Sdk.Contracts.Keys;

namespace JobMaster.Sdk.Ioc.Setup.Selectors;

internal sealed class AgentConnectionConfigSelector : IAgentConnectionConfigSelectorAdvanced
{
    private readonly ClusterConfigBuilder root;
    private readonly AgentConnectionDefinition def;

    public AgentConnectionConfigSelector(ClusterConfigBuilder root, AgentConnectionDefinition def)
    {
        this.root = root;
        this.def = def;
    }

    public IAgentConnectionConfigSelector AgentConnName(string agentConnName)
    {
        def.AgentConnectionName = agentConnName;
        return this;
    }

    public IAgentConnectionConfigSelector AgentRepoType(string repoType)
    {
        def.AgentRepoType = repoType;
        return this;
    }

    public IAgentConnectionConfigSelector AgentConnString(string connString)
    {
        def.AgentConnString = connString;
        return this;
    }

    public IAgentConnectionConfigSelector AgentDbOperationThrottleLimit(int limit)
    {
        def.RuntimeDbOperationThrottleLimit = limit;
        return this;
    }

    public IAgentConnectionConfigSelector AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig)
    {
        def.AgentAdditionalConnConfig = additionalConnConfig;
        return this;
    }

    public IAgentConnectionConfigSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value)
    {
        if (def.AgentAdditionalConnConfig is null)
        {
            def.AgentAdditionalConnConfig = new JobMasterConfigDictionary();
        }
        
        def.AgentAdditionalConnConfig.SetValue(namespaceKey, key, value);
        return this;
    }

    public IAgentConnectionConfigSelector AgentRuntimeDbOperationThrottleLimit(int runtimeDbOperationThrottleLimit)
    {
        def.RuntimeDbOperationThrottleLimit = runtimeDbOperationThrottleLimit;
        return this;
    }
}