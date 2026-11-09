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
