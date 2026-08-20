using System.Security.Cryptography.X509Certificates;
using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;

namespace JobMaster.RavenDb;

/// <summary>
/// Extension methods for configuring RavenDB as the JobMaster storage backend.
/// </summary>
public static class ConfigExtensions
{
    /// <summary>
    /// Configures the cluster master database to use RavenDB.
    /// </summary>
    /// <param name="connectionString">Format: "Urls=url1,url2;Database=name" -- just the connection
    /// target, not authentication. See <paramref name="certificate"/> for client-certificate auth.</param>
    /// <param name="certificate">Client certificate for RavenDB's X.509 authentication, if the server
    /// requires one. Pass an already-loaded <see cref="X509Certificate2"/> (from a cert store, Key Vault,
    /// wherever) -- unlike the connection string, this can't be expressed as plain text.</param>
    /// <param name="collectionPrefix">Custom collection-name prefix for all JobMaster collections created
    /// in the master database. Defaults to <c>JM_</c> when not specified. Exists for the same reason as
    /// SQL's table prefix: avoid colliding with unrelated data if the database is shared/reused, not for
    /// separating multiple clusters -- that's what the compound document ID (which embeds ClusterId)
    /// already does.</param>
    /// <param name="enableDocumentExpiration">Opts the master database into RavenDB's native
    /// document-expiration background job (disabled by default on every RavenDB database). Without this,
    /// <c>@expires</c> metadata -- currently only used by the distributed locker's zombie-lock backstop --
    /// is inert; nothing ever physically deletes those entries. Not enabled automatically because it's a
    /// database-wide setting, not scoped to JobMaster's own collections, and forcing it on could conflict
    /// with an operator already managing expiration for other data sharing this database. RavenDB
    /// Community licenses cap the sweep frequency at a 36-hour minimum -- fine for this backstop's purpose
    /// (housekeeping for entries nobody ever explicitly released or stole, not a latency-sensitive path),
    /// so enabling this always requests that floor rather than exposing a configurable frequency.</param>
    public static T UseRavenDb<T>(
        this T clusterConfigSelector,
        string connectionString,
        X509Certificate2? certificate = null,
        string? collectionPrefix = null,
        bool enableDocumentExpiration = false)
        where T : IBaseClusterConfigSelector<IClusterConfigSelector>
    {
        clusterConfigSelector.ClusterConnString(connectionString);
        clusterConfigSelector.ClusterRepoType(RavenDbRepositoryConstants.RepositoryTypeId);

        if (certificate != null)
        {
            clusterConfigSelector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CertificateKey, certificate);
        }

        if (collectionPrefix != null)
        {
            clusterConfigSelector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey, collectionPrefix);
        }

        if (enableDocumentExpiration)
        {
            clusterConfigSelector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.EnableDocumentExpirationKey, true);
        }

        return clusterConfigSelector;
    }

    /// <summary>
    /// Configures an agent connection to use RavenDB. See <see cref="UseRavenDb{T}"/> for parameter
    /// details -- this is the agent-side counterpart; there's no document-expiration option here since
    /// agent messages never use <c>@expires</c>.
    /// </summary>
    public static IAgentConnectionConfigSelector UseRavenDb(
        this IAgentConnectionConfigSelector agentConfigSelector,
        string connectionString,
        X509Certificate2? certificate = null,
        string? collectionPrefix = null)
    {
        agentConfigSelector.AgentConnString(connectionString);
        agentConfigSelector.AgentRepoType(RavenDbRepositoryConstants.RepositoryTypeId);

        if (certificate != null)
        {
            agentConfigSelector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CertificateKey, certificate);
        }

        if (collectionPrefix != null)
        {
            agentConfigSelector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey, collectionPrefix);
        }

        return agentConfigSelector;
    }

    internal static string GetCollectionPrefix(this JobMasterClusterConnectionConfig clusterConnectionConfig)
    {
        return clusterConnectionConfig.AdditionalConnConfig.TryGetValue<string>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey)
               ?? RavenDbConfigKeys.DefaultCollectionPrefix;
    }

    internal static bool IsDocumentExpirationEnabled(this JobMasterClusterConnectionConfig clusterConnectionConfig)
    {
        return clusterConnectionConfig.AdditionalConnConfig.TryGetValue<bool?>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.EnableDocumentExpirationKey)
               ?? false;
    }

    internal static X509Certificate2? GetCertificate(this JobMasterClusterConnectionConfig clusterConnectionConfig)
    {
        return clusterConnectionConfig.AdditionalConnConfig.TryGetValue<X509Certificate2>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CertificateKey);
    }

    // Agent-side counterparts -- RavenDbAgentFingerprintResolver/RavenDbRawMessagesDispatcherRepository
    // only ever have a JobMasterAgentConnectionConfig in scope (via Initialize), never a
    // JobMasterClusterConnectionConfig.
    internal static string GetCollectionPrefix(this JobMasterAgentConnectionConfig agentConnectionConfig)
    {
        return agentConnectionConfig.AdditionalConnConfig.TryGetValue<string>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey)
               ?? RavenDbConfigKeys.DefaultCollectionPrefix;
    }

    internal static X509Certificate2? GetCertificate(this JobMasterAgentConnectionConfig agentConnectionConfig)
    {
        return agentConnectionConfig.AdditionalConnConfig.TryGetValue<X509Certificate2>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CertificateKey);
    }
}
