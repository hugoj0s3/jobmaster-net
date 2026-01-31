using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;

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
    
    public static IClusterStandaloneConfigSelector UseSqlServer(
        this IClusterStandaloneConfigSelector standaloneConfigSelector, 
        string connectionString)
    {
        standaloneConfigSelector.ClusterConnString(connectionString);
        standaloneConfigSelector.ClusterRepoType(SqlServerRepositoryConstants.RepositoryTypeId);
        return standaloneConfigSelector;
    }
}
