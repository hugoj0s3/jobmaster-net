using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.RavenDb;

/// <summary>
/// Extension methods for configuring RavenDB as the JobMaster storage backend.
/// </summary>
public static class ConfigExtensions
{
    /// <summary>
    /// Configures the cluster master database to use RavenDB with the given connection string
    /// (format: "Urls=url1,url2;Database=name").
    /// </summary>
    public static T UseRavenDbForMaster<T>(this T clusterConfigSelector, string connectionString)
        where T : IBaseClusterConfigSelector<IClusterConfigSelector>
    {
        clusterConfigSelector.ClusterConnString(connectionString);
        clusterConfigSelector.ClusterRepoType(RavenDbRepositoryConstants.RepositoryTypeId);
        return clusterConfigSelector;
    }

    /// <summary>
    /// Sets a custom collection-name prefix for all JobMaster collections created in the master
    /// database. Defaults to <c>JM_</c> when not specified. Exists for the same reason as SQL's
    /// table prefix: avoid colliding with unrelated data if the database is shared/reused, not for
    /// separating multiple clusters -- that's what the compound document ID (which embeds ClusterId)
    /// already does.
    /// </summary>
    public static T UseRavenDbCollectionPrefixForMaster<T>(this T clusterConfigSelector, string collectionPrefix)
        where T : IBaseClusterConfigSelector<IClusterConfigSelector>
    {
        clusterConfigSelector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey, collectionPrefix);
        return clusterConfigSelector;
    }

    internal static string GetCollectionPrefix(this JobMasterClusterConnectionConfig clusterConnectionConfig)
    {
        return clusterConnectionConfig.AdditionalConnConfig.TryGetValue<string>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey)
               ?? RavenDbConfigKeys.DefaultCollectionPrefix;
    }
}
