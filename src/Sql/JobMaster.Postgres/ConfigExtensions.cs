using JobMaster.Contracts.Ioc.Selectors;
using JobMaster.Sdk.Contracts.Ioc.Selectors;

namespace JobMaster.Postgres;

public static class ConfigExtensions
{
    public static IClusterConfigSelector UsePostgresForMaster(this IClusterConfigSelector clusterConfigSelector, string connectionString)
    {
        ((IClusterConfigSelectorAdvanced)clusterConfigSelector).ClusterConnString(connectionString);
        ((IClusterConfigSelectorAdvanced)clusterConfigSelector).ClusterRepoType(PostgresRepositoryConstants.RepositoryTypeId);
        
        return clusterConfigSelector;
    }
    
    public static IAgentConnectionConfigSelector UsePostgresForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string connectionString)
    {
        ((IAgentConnectionConfigSelectorAdvanced)agentConfigSelector).AgentConnString(connectionString);
        ((IAgentConnectionConfigSelectorAdvanced)agentConfigSelector).AgentRepoType(PostgresRepositoryConstants.RepositoryTypeId);
        return agentConfigSelector;
    }
}