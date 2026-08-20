using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.RavenDb;

/// <summary>
/// Shared naming scheme for every RavenDB domain: one collection per logical domain
/// (<paramref name="collection"/>), prefixed to avoid colliding with unrelated data in a shared
/// database, with document IDs compounded by <c>ClusterId</c> so cluster-scoped bulk operations are a
/// cheap ID-prefix stream instead of a collection scan + field filter.
/// </summary>
internal static class RavenDbDocumentNaming
{
    public static string CollectionName(JobMasterClusterConnectionConfig clusterConnConfig, string collection) =>
        $"{clusterConnConfig.GetCollectionPrefix()}{collection}";

    public static string DocumentId(JobMasterClusterConnectionConfig clusterConnConfig, string collection, string entityId) =>
        DocumentId(clusterConnConfig.GetCollectionPrefix(), clusterConnConfig.ClusterId, collection, entityId);

    // Low-level form for callers that don't have a JobMasterClusterConnectionConfig in scope --
    // RavenDbAgentFingerprintResolver only gets a JobMasterAgentConnectionConfig (via Initialize) plus
    // clusterId/agentConnectionId as method parameters, never a cluster-aware config.
    public static string DocumentId(string prefix, string clusterId, string collection, string entityId) =>
        $"{prefix}{collection}/{clusterId}/{entityId}";
}
