using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;
using JobMaster.SqlBase;

namespace JobMaster.MySql;

/// <summary>
/// Extension methods for configuring MySQL as the JobMaster storage backend.
/// </summary>
public static class ConfigExtensions
{
    /// <summary>
    /// Configures the cluster master database to use MySQL with the given connection string.
    /// </summary>
    /// <param name="tablePrefix">Custom table prefix for all JobMaster tables created in this database. Defaults to <c>JM_</c> when not specified.</param>
    public static T UseMySqlForMaster<T>(this T clusterConfigSelector, string connectionString, string? tablePrefix = null)
        where T : IBaseClusterConfigSelector<IClusterConfigSelector>
    {
        clusterConfigSelector.ClusterConnString(connectionString);
        clusterConfigSelector.ClusterRepoType(MySqlRepositoryConstants.RepositoryTypeId);
        if (tablePrefix != null)
            clusterConfigSelector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, tablePrefix);
        return clusterConfigSelector;
    }

    /// <summary>
    /// Configures an agent connection to use MySQL with the given connection string.
    /// </summary>
    /// <param name="tablePrefix">Custom table prefix for all JobMaster tables created in this database. Defaults to <c>JM_</c> when not specified.</param>
    public static IAgentConnectionConfigSelector UseMySqlForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string connectionString, string? tablePrefix = null)
    {
        agentConfigSelector.AgentConnString(connectionString);
        agentConfigSelector.AgentRepoType(MySqlRepositoryConstants.RepositoryTypeId);
        if (tablePrefix != null)
            agentConfigSelector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, tablePrefix);
        return agentConfigSelector;
    }

    /// <summary>
    /// Configures a standalone cluster to use MySQL for both the master database and the agent,
    /// using a single shared connection string.
    /// </summary>
    /// <param name="tablePrefix">Custom table prefix for all JobMaster tables created in this database. Defaults to <c>JM_</c> when not specified.</param>
    public static IClusterStandaloneConfigSelector UseMySql(
        this IClusterStandaloneConfigSelector standaloneConfigSelector,
        string connectionString,
        string? tablePrefix = null)
    {
        standaloneConfigSelector.ClusterConnString(connectionString);
        standaloneConfigSelector.ClusterRepoType(MySqlRepositoryConstants.RepositoryTypeId);
        if (tablePrefix != null)
            standaloneConfigSelector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, tablePrefix);
        return standaloneConfigSelector;
    }
}
