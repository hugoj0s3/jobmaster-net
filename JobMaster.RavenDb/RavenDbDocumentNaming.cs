using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.RavenDb;

/// <summary>
/// Shared naming scheme for every RavenDB domain (Jobs, RecurringSchedules, GenericRecords, Logs, ...):
/// one RavenDB collection per logical domain (<paramref name="collection"/>), prefixed to avoid
/// colliding with unrelated data in a shared/reused database, with document IDs compounded by
/// <c>ClusterId</c> -- mirrors <c>RavenDbMasterDistributedLockerRepository</c>'s
/// <c>lock/{ClusterId}/{key}</c> compare-exchange key exactly, for the same two reasons: every
/// repository already has <c>ClusterId</c> in scope at every call site, and it makes cluster-scoped
/// bulk operations a cheap ID-prefix stream instead of a collection scan + field filter.
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
