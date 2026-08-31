using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;

namespace JobMaster.SqlBase;

/// <summary>
/// Extension methods for shared SQL configuration options available to all SQL-backed providers
/// (PostgreSQL, SQL Server, MySQL).
/// </summary>
public static class ConfigExtensions
{
    /// <summary>
    /// Sets a custom table prefix for all JobMaster tables created in the master database.
    /// Defaults to <c>JM_</c> when not specified.
    /// </summary>
    [Obsolete("Use the tablePrefix parameter on UsePostgresForMaster/UseMySqlForMaster/UseSqlServerForMaster " +
              "(or UsePostgres/UseMySql/UseSqlServer for standalone clusters) instead — a single provider-scoped " +
              "setting is clearer than one that applies regardless of which provider is actually in use. " +
              "This method will be removed in a future release.")]
    public static T UseSqlTablePrefixForMaster<T>(this T clusterConfigSelector, string tablePrefix)
        where T : IBaseClusterConfigSelector<IClusterConfigSelector>
    {
        clusterConfigSelector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, tablePrefix);
        return clusterConfigSelector;
    }

    /// <summary>
    /// Sets a custom table prefix for all JobMaster tables created in the agent database.
    /// Defaults to <c>JM_</c> when not specified.
    /// </summary>
    [Obsolete("Use the tablePrefix parameter on UsePostgresForAgent/UseMySqlForAgent/UseSqlServerForAgent instead. " +
              "This method will be removed in a future release.")]
    public static IAgentConnectionConfigSelector UseSqlTablePrefixForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string tablePrefix)
    {
        agentConfigSelector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, tablePrefix);
        return agentConfigSelector;
    }

    /// <summary>
    /// Disables automatic schema provisioning on startup.
    /// Use this when you manage the database schema yourself (e.g. via migrations)
    /// and do not want JobMaster to create or alter tables.
    /// </summary>
    public static T DisableAutoProvisionSqlSchema<T>(this T clusterConfigSelector)
        where T : IBaseClusterConfigSelector<IClusterConfigSelector>
    {
        clusterConfigSelector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.DisableAutoProvisionSchemaKey, true);
        return clusterConfigSelector;
    }

    internal static bool IsAutoProvisionSqlSchemaEnabled(this JobMasterClusterConnectionConfig clusterConnectionConfig)
    {
        return clusterConnectionConfig.AdditionalConnConfig.TryGetValue<bool>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.DisableAutoProvisionSchemaKey) != true;
    }
}
