using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;

namespace JobMaster.Abstractions.Ioc.Selectors;

/// <summary>
/// Provider-selection contract for an agent connection target (either a standalone agent connection
/// or the transport/execution leg of a hybrid agent connection).
/// </summary>
public interface IAgentConnectionProviderSelector
{
    internal IAgentConnectionProviderSelector AgentAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig);

    internal IAgentConnectionProviderSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);

    internal IAgentConnectionProviderSelector AgentRepoType(string repoType);

    internal IAgentConnectionProviderSelector AgentConnString(string connString);
}
