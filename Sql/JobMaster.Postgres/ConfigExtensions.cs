using JobMaster.Contracts.Ioc.Selectors;

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
}