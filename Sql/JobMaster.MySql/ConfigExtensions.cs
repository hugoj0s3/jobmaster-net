using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;

namespace JobMaster.MySql;

/// <summary>
/// Extension methods for configuring MySQL as the JobMaster storage backend.
/// </summary>
public static class ConfigExtensions
{
    /// <summary>
    /// Configures the cluster master database to use MySQL with the given connection string.
    /// </summary>
    public static T UseMySqlForMaster<T>(this T clusterConfigSelector, string connectionString)
        where T : IBaseClusterConfigSelector<IClusterConfigSelector>
    {
        clusterConfigSelector.ClusterConnString(connectionString);
        clusterConfigSelector.ClusterRepoType(MySqlRepositoryConstants.RepositoryTypeId);
        return clusterConfigSelector;
    }

    /// <summary>
    /// Configures an agent connection to use MySQL with the given connection string.
    /// </summary>
    public static IAgentConnectionConfigSelector UseMySqlForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string connectionString)
    {
        agentConfigSelector.AgentConnString(connectionString);
        agentConfigSelector.AgentRepoType(MySqlRepositoryConstants.RepositoryTypeId);
        return agentConfigSelector;
    }

    /// <summary>
    /// Configures a standalone cluster to use MySQL for both the master database and the agent,
    /// using a single shared connection string.
    /// </summary>
    public static IClusterStandaloneConfigSelector UseMySql(
        this IClusterStandaloneConfigSelector standaloneConfigSelector,
        string connectionString)
    {
        standaloneConfigSelector.ClusterConnString(connectionString);
        standaloneConfigSelector.ClusterRepoType(MySqlRepositoryConstants.RepositoryTypeId);
        return standaloneConfigSelector;
    }
}
