using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;

namespace JobMaster.SqlServer;

/// <summary>
/// Extension methods for configuring SQL Server as the JobMaster storage backend.
/// </summary>
public static class ConfigExtensions
{
    /// <summary>
    /// Configures the cluster master database to use SQL Server with the given connection string.
    /// </summary>
    public static T UseSqlServerForMaster<T>(this T clusterConfigSelector, string connectionString)
        where T : IBaseClusterConfigSelector<IClusterConfigSelector>
    {
        clusterConfigSelector.ClusterConnString(connectionString);
        clusterConfigSelector.ClusterRepoType(SqlServerRepositoryConstants.RepositoryTypeId);
        return clusterConfigSelector;
    }

    /// <summary>
    /// Configures an agent connection to use SQL Server with the given connection string.
    /// </summary>
    public static IAgentConnectionConfigSelector UseSqlServerForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string connectionString)
    {
        agentConfigSelector.AgentConnString(connectionString);
        agentConfigSelector.AgentRepoType(SqlServerRepositoryConstants.RepositoryTypeId);
        return agentConfigSelector;
    }

    /// <summary>
    /// Configures a standalone cluster to use SQL Server for both the master database and the agent,
    /// using a single shared connection string.
    /// </summary>
    public static IClusterStandaloneConfigSelector UseSqlServer(
        this IClusterStandaloneConfigSelector standaloneConfigSelector,
        string connectionString)
    {
        standaloneConfigSelector.ClusterConnString(connectionString);
        standaloneConfigSelector.ClusterRepoType(SqlServerRepositoryConstants.RepositoryTypeId);
        return standaloneConfigSelector;
    }
}
