using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Reflection;

namespace JobMaster.Sdk.Ioc.Setup.Selectors;

internal sealed class AgentConnectionProviderSelector : IAgentConnectionProviderSelector
{
    private readonly AgentProviderDefinition provider;

    public AgentConnectionProviderSelector(AgentProviderDefinition provider)
    {
        this.provider = provider;
    }

    public IAgentConnectionProviderSelector AgentRepoType(string repoType)
    {
        provider.AgentRepoType = ProviderAssemblies.ToKnownRepositoryType(repoType);
        return this;
    }

    public IAgentConnectionProviderSelector AgentConnString(string connString)
    {
        provider.AgentConnString = connString;
        return this;
    }

    public IAgentConnectionProviderSelector AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig)
    {
        provider.AgentAdditionalConnConfig = additionalConnConfig;
        return this;
    }

    public IAgentConnectionProviderSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value)
    {
        provider.AgentAdditionalConnConfig ??= new JobMasterConfigDictionary();
        provider.AgentAdditionalConnConfig.SetValue(namespaceKey, key, value);
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

    // Single-provider setters write into the shared provider (def.Transport == def.Execution).
    public IAgentConnectionProviderSelector AgentRepoType(string repoType)
    {
        SingleProvider().AgentRepoType = ProviderAssemblies.ToKnownRepositoryType(repoType);
        return this;
    }

    public IAgentConnectionProviderSelector AgentConnString(string connString)
    {
        SingleProvider().AgentConnString = connString;
        return this;
    }

    public IAgentConnectionProviderSelector AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig)
    {
        SingleProvider().AgentAdditionalConnConfig = additionalConnConfig;
        return this;
    }

    public IAgentConnectionProviderSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value)
    {
        var provider = SingleProvider();
        provider.AgentAdditionalConnConfig ??= new JobMasterConfigDictionary();
        provider.AgentAdditionalConnConfig.SetValue(namespaceKey, key, value);
        return this;
    }

    private AgentProviderDefinition SingleProvider()
    {
        if (def.IsHybrid)
        {
            throw new InvalidOperationException("Cannot configure both a single provider and UseHybrid() on the same agent connection.");
        }

        return def.Transport;
    }
}
