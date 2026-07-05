using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;

namespace JobMaster.Abstractions.Ioc.Selectors;

/// <summary>
/// Minimal fluent configuration entry point for connecting a JobMaster cluster.
/// Contains only cluster identity and connection methods — no workers, no agent connections,
/// no execution policy, no data-retention settings.
/// Use this interface when registering a cluster for API-only access (monitoring and operations
/// without job processing). For full cluster setup use <see cref="IClusterConfigSelector"/>.
/// All methods return the same selector instance to allow method chaining.
/// </summary>
public interface IBaseClusterConfigSelector<TSelector> where TSelector : IBaseClusterConfigSelector<TSelector>
{
    /// <summary>
    /// Marks this cluster as the default cluster.
    /// When multiple clusters are registered, the default is used whenever no explicit cluster ID is specified.
    /// </summary>
    public TSelector SetAsDefault();

    /// <summary>
    /// Sets the unique identifier for this cluster.
    /// This ID is used to route jobs and distinguish clusters in a multi-cluster setup.
    /// </summary>
    /// <param name="clusterId">A unique string that identifies this cluster.</param>
    public TSelector ClusterId(string clusterId);

    internal TSelector ClusterRepoType(string repoType);
    internal TSelector ClusterConnString(string connString);
    internal TSelector ClusterAdditionalConnConfig(JobMasterConfigDictionary additionalConnConfig);
    internal TSelector AppendAdditionalConnConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);
    internal TSelector ClusterAdditionalConfig(JobMasterConfigDictionary additionalConfig);
    internal TSelector AppendAdditionalConfigValue(JobMasterNamespaceUniqueKey namespaceKey, string key, object value);
    internal TSelector ClusterRuntimeDbOperationLimit(int runtimeDbOperationThrottleLimit);
    internal void Finish();
}
