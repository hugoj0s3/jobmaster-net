using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;

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
    
    public static IClusterStandaloneConfigSelector UseMySqlForMaster(
        this IClusterStandaloneConfigSelector standaloneConfigSelector, 
        string connectionString)
    {
       standaloneConfigSelector.ClusterConnString(connectionString);
       standaloneConfigSelector.ClusterRepoType(MySqlRepositoryConstants.RepositoryTypeId);
       return standaloneConfigSelector;
    }
    
}
