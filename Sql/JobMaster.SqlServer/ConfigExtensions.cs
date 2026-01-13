using JobMaster.Contracts.Ioc.Selectors;

namespace JobMaster.SqlServer;

public static class ConfigExtensions
{
    public static IClusterConfigSelector UseSqlServerForMaster(this IClusterConfigSelector clusterConfigSelector, string connectionString)
    {
        clusterConfigSelector.ClusterConnString(connectionString);
        clusterConfigSelector.ClusterRepoType(SqlServerRepositoryConstants.RepositoryTypeId);
        return clusterConfigSelector;
    }

    public static IAgentConnectionConfigSelector UseSqlServerForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string connectionString)
    {
        agentConfigSelector.AgentConnString(connectionString);
        agentConfigSelector.AgentRepoType(SqlServerRepositoryConstants.RepositoryTypeId);
        return agentConfigSelector;
    }
}
