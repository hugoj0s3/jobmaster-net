using JobMaster.Abstractions.Ioc.Selectors;

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
}
