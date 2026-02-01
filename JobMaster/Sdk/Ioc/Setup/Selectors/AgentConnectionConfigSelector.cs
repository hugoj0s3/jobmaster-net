using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Keys;

namespace JobMaster.Sdk.Ioc.Setup.Selectors;

internal sealed class AgentConnectionConfigSelector : IAgentConnectionConfigSelector
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

    public IAgentConnectionConfigSelector RuntimeDbOperationThrottleLimit(int limit)
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