using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;

namespace JobMaster.Postgres;

public static class ConfigExtensions
{
    public static IClusterConfigSelector UsePostgresForMaster(this IClusterConfigSelector clusterConfigSelector, string connectionString)
    {
        clusterConfigSelector.ClusterConnString(connectionString);
        clusterConfigSelector.ClusterRepoType(PostgresRepositoryConstants.RepositoryTypeId);
        
        return clusterConfigSelector;
    }
    
    public static IAgentConnectionConfigSelector UsePostgresForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string connectionString)
    {
        agentConfigSelector.AgentConnString(connectionString);
        agentConfigSelector.AgentRepoType(PostgresRepositoryConstants.RepositoryTypeId);
        return agentConfigSelector;
    }
    
    public static IClusterStandaloneConfigSelector UsePostgres(
        this IClusterStandaloneConfigSelector standaloneConfigSelector, 
        string connectionString)
    {
        standaloneConfigSelector.ClusterConnString(connectionString);
        standaloneConfigSelector.ClusterRepoType(PostgresRepositoryConstants.RepositoryTypeId);
        return standaloneConfigSelector;
    }
}