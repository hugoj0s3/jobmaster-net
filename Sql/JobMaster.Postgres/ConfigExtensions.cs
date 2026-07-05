using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;

namespace JobMaster.Postgres;

/// <summary>
/// Extension methods for configuring PostgreSQL as the JobMaster storage backend.
/// </summary>
public static class ConfigExtensions
{
    /// <summary>
    /// Configures the cluster master database to use PostgreSQL with the given connection string.
    /// </summary>
    public static T UsePostgresForMaster<T>(this T clusterConfigSelector, string connectionString)
        where T : IBaseClusterConfigSelector<IClusterConfigSelector>
    {
        clusterConfigSelector.ClusterConnString(connectionString);
        clusterConfigSelector.ClusterRepoType(PostgresRepositoryConstants.RepositoryTypeId);
        return clusterConfigSelector;
    }

    /// <summary>
    /// Configures an agent connection to use PostgreSQL with the given connection string.
    /// </summary>
    public static IAgentConnectionConfigSelector UsePostgresForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string connectionString)
    {
        agentConfigSelector.AgentConnString(connectionString);
        agentConfigSelector.AgentRepoType(PostgresRepositoryConstants.RepositoryTypeId);
        return agentConfigSelector;
    }

    /// <summary>
    /// Configures a standalone cluster to use PostgreSQL for both the master database and the agent,
    /// using a single shared connection string.
    /// </summary>
    public static IClusterStandaloneConfigSelector UsePostgres(
        this IClusterStandaloneConfigSelector standaloneConfigSelector,
        string connectionString)
    {
        standaloneConfigSelector.ClusterConnString(connectionString);
        standaloneConfigSelector.ClusterRepoType(PostgresRepositoryConstants.RepositoryTypeId);
        return standaloneConfigSelector;
    }
}
