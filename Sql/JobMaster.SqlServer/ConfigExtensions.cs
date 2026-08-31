using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;
using JobMaster.SqlBase;

namespace JobMaster.SqlServer;

/// <summary>
/// Extension methods for configuring SQL Server as the JobMaster storage backend.
/// </summary>
public static class ConfigExtensions
{
    /// <summary>
    /// Configures the cluster master database to use SQL Server with the given connection string.
    /// </summary>
    /// <param name="tablePrefix">Custom table prefix for all JobMaster tables created in this database. Defaults to <c>JM_</c> when not specified.</param>
    public static T UseSqlServerForMaster<T>(this T clusterConfigSelector, string connectionString, string? tablePrefix = null)
        where T : IBaseClusterConfigSelector<IClusterConfigSelector>
    {
        clusterConfigSelector.ClusterConnString(connectionString);
        clusterConfigSelector.ClusterRepoType(SqlServerRepositoryConstants.RepositoryTypeId);
        if (tablePrefix != null)
            clusterConfigSelector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, tablePrefix);
        return clusterConfigSelector;
    }

    /// <summary>
    /// Configures an agent connection to use SQL Server with the given connection string.
    /// </summary>
    /// <param name="tablePrefix">Custom table prefix for all JobMaster tables created in this database. Defaults to <c>JM_</c> when not specified.</param>
    public static IAgentConnectionConfigSelector UseSqlServerForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string connectionString, string? tablePrefix = null)
    {
        agentConfigSelector.AgentConnString(connectionString);
        agentConfigSelector.AgentRepoType(SqlServerRepositoryConstants.RepositoryTypeId);
        if (tablePrefix != null)
            agentConfigSelector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, tablePrefix);
        return agentConfigSelector;
    }

    /// <summary>
    /// Configures a standalone cluster to use SQL Server for both the master database and the agent,
    /// using a single shared connection string.
    /// </summary>
    /// <param name="tablePrefix">Custom table prefix for all JobMaster tables created in this database. Defaults to <c>JM_</c> when not specified.</param>
    public static IClusterStandaloneConfigSelector UseSqlServer(
        this IClusterStandaloneConfigSelector standaloneConfigSelector,
        string connectionString,
        string? tablePrefix = null)
    {
        standaloneConfigSelector.ClusterConnString(connectionString);
        standaloneConfigSelector.ClusterRepoType(SqlServerRepositoryConstants.RepositoryTypeId);
        if (tablePrefix != null)
            standaloneConfigSelector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, tablePrefix);
        return standaloneConfigSelector;
    }
}
