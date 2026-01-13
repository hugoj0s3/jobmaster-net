using JobMaster.Contracts.Ioc.Selectors;

namespace JobMaster.MySql;

public static class ConfigExtensions
{
    public static IClusterConfigSelector UseMySqlForMaster(this IClusterConfigSelector clusterConfigSelector, string connectionString)
    {
        clusterConfigSelector.ClusterConnString(connectionString);
        clusterConfigSelector.ClusterRepoType(MySqlRepositoryConstants.RepositoryTypeId);
        return clusterConfigSelector;
    }

    public static IAgentConnectionConfigSelector UseMySqlForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string connectionString)
    {
        agentConfigSelector.AgentConnString(connectionString);
        agentConfigSelector.AgentRepoType(MySqlRepositoryConstants.RepositoryTypeId);
        return agentConfigSelector;
    }
}
