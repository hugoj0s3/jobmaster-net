using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;

namespace JobMaster.SqlServer;

public static class ConfigExtensions
{
    public static IClusterConfigSelector UseSqlServerForMaster(this IClusterConfigSelector clusterConfigSelector, string connectionString)
    {
        ((IClusterConfigSelectorAdvanced)clusterConfigSelector).ClusterConnString(connectionString);
        ((IClusterConfigSelectorAdvanced)clusterConfigSelector).ClusterRepoType(SqlServerRepositoryConstants.RepositoryTypeId);
        return clusterConfigSelector;
    }

    public static IAgentConnectionConfigSelector UseSqlServerForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string connectionString)
    {
        ((IAgentConnectionConfigSelectorAdvanced)agentConfigSelector).AgentConnString(connectionString);
        ((IAgentConnectionConfigSelectorAdvanced)agentConfigSelector).AgentRepoType(SqlServerRepositoryConstants.RepositoryTypeId);
        return agentConfigSelector;
    }
}
