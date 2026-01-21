using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;

namespace JobMaster.Sql;

public static class ConfigExtensions
{
    public static IClusterConfigSelector UseSqlTablePrefixForMaster(this IClusterConfigSelector clusterConfigSelector, string tablePrefix)
    {
        var advancedSelector = (IClusterConfigSelectorAdvanced) clusterConfigSelector;
        advancedSelector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, tablePrefix);
        return clusterConfigSelector;
    }

    public static IAgentConnectionConfigSelector UseSqlTablePrefixForAgent(this IAgentConnectionConfigSelector agentConfigSelector, string tablePrefix)
    {
        var advancedSelector = (IAgentConnectionConfigSelectorAdvanced) agentConfigSelector;
        advancedSelector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, tablePrefix);
        return agentConfigSelector;
    }
    
    public static IClusterConfigSelector DisableAutoProvisionSqlSchema(this IClusterConfigSelector clusterConfigSelector)
    {
        var advancedSelector = (IClusterConfigSelectorAdvanced) clusterConfigSelector;
        advancedSelector.AppendAdditionalConnConfigValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.DisableAutoProvisionSchemaKey, true);
        return clusterConfigSelector;
    }
    
    public static bool IsAutoProvisionSqlSchemaEnabled(this JobMasterClusterConnectionConfig clusterConnectionConfig)
    {
        return clusterConnectionConfig.AdditionalConnConfig.TryGetValue<bool>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.DisableAutoProvisionSchemaKey) != true;
    }
}