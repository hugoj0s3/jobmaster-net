using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Keys;

namespace JobMaster.Sdk.Ioc.Setup.Selectors;

internal sealed class AgentConnectionProviderSelector : IAgentConnectionProviderSelector
{
    private readonly AgentConnectionTargetDefinition target;

    public AgentConnectionProviderSelector(AgentConnectionTargetDefinition target)
    {
        this.target = target;
    }

    public IAgentConnectionProviderSelector AgentRepoType(string repoType)
    {
        target.AgentRepoType = repoType;
        return this;
    }

    public IAgentConnectionProviderSelector AgentConnString(string connString)
    {
        target.AgentConnString = connString;
        return this;
    }

    public IAgentConnectionProviderSelector AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig)
    {
        target.AgentAdditionalConnConfig = additionalConnConfig;
        return this;
    }

    public IAgentConnectionProviderSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value)
    {
        target.AgentAdditionalConnConfig ??= new JobMasterConfigDictionary();
        target.AgentAdditionalConnConfig.SetValue(namespaceKey, key, value);
        return this;
    }
}

internal sealed class HybridAgentConnectionConfigSelector : IHybridAgentConnectionConfigSelector
{
    private readonly AgentConnectionDefinition def;

    public HybridAgentConnectionConfigSelector(AgentConnectionDefinition def)
    {
        this.def = def;
    }

    public IHybridAgentConnectionConfigSelector AgentConnName(string agentConnName)
    {
        def.AgentConnectionName = agentConnName;
        return this;
    }

    public IHybridAgentConnectionConfigSelector ProtectConnectionChanges(bool protectConnectionChanges)
    {
        def.ProtectConnectionChanges = protectConnectionChanges;
        return this;
    }

    public IHybridAgentConnectionConfigSelector Transport(Action<IAgentConnectionProviderSelector> configure)
    {
        configure(new AgentConnectionProviderSelector(def.Transport));
        return this;
    }

    public IHybridAgentConnectionConfigSelector Execution(Action<IAgentConnectionProviderSelector> configure)
    {
        configure(new AgentConnectionProviderSelector(def.Execution));
        return this;
    }
}

internal sealed class AgentConnectionConfigSelector : IAgentConnectionConfigSelector
{
    private readonly AgentConnectionDefinition def;

    public AgentConnectionConfigSelector(AgentConnectionDefinition def)
    {
        this.def = def;
    }

    public IAgentConnectionConfigSelector AgentConnName(string agentConnName)
    {
        def.AgentConnectionName = agentConnName;
        return this;
    }

    public IAgentConnectionConfigSelector ProtectConnectionChanges(bool protectConnectionChanges)
    {
        def.ProtectConnectionChanges = protectConnectionChanges;
        return this;
    }

    public IHybridAgentConnectionConfigSelector UseHybrid()
    {
        def.UseHybrid();
        return new HybridAgentConnectionConfigSelector(def);
    }

    // Single-provider setters write into the shared target (def.Transport == def.Execution).
    public IAgentConnectionProviderSelector AgentRepoType(string repoType)
    {
        SingleTarget().AgentRepoType = repoType;
        return this;
    }

    public IAgentConnectionProviderSelector AgentConnString(string connString)
    {
        SingleTarget().AgentConnString = connString;
        return this;
    }

    public IAgentConnectionProviderSelector AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig)
    {
        SingleTarget().AgentAdditionalConnConfig = additionalConnConfig;
        return this;
    }

    public IAgentConnectionProviderSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value)
    {
        var target = SingleTarget();
        target.AgentAdditionalConnConfig ??= new JobMasterConfigDictionary();
        target.AgentAdditionalConnConfig.SetValue(namespaceKey, key, value);
        return this;
    }

    private AgentConnectionTargetDefinition SingleTarget()
    {
        if (def.IsHybrid)
        {
            throw new InvalidOperationException("Cannot configure both a single provider and UseHybrid() on the same agent connection.");
        }

        return def.Transport;
    }
}
