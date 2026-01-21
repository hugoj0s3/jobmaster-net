using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;

namespace JobMaster.MySql;

public static class ConfigExtensions
{
    public static IClusterConfigSelector UseMySqlForMaster(this IClusterConfigSelector clusterConfigSelector, string connectionString)
    {
        ((IClusterConfigSelectorAdvanced)clusterConfigSelector).ClusterConnString(connectionString);
        ((IClusterConfigSelectorAdvanced)clusterConfigSelector).ClusterRepoType(MySqlRepositoryConstants.RepositoryTypeId);
        return clusterConfigSelector;
    }

    public static IAgentConnectionConfigSelector UseMySqlForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string connectionString)
    {
        ((IAgentConnectionConfigSelectorAdvanced)agentConfigSelector).AgentConnString(connectionString);
        ((IAgentConnectionConfigSelectorAdvanced)agentConfigSelector).AgentRepoType(MySqlRepositoryConstants.RepositoryTypeId);
        return agentConfigSelector;
    }
}
