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
    /// <param name="connectionString">Format: "Urls=url1,url2;Database=name".</param>
    /// <param name="certificate">Client certificate for RavenDB's X.509 authentication, if the server requires one.</param>
    /// <param name="collectionPrefix">Custom collection-name prefix for all JobMaster collections. Defaults to <c>JM_</c>.</param>
    /// <param name="enableDocumentExpiration">Opts the master database into RavenDB's native
    /// document-expiration background job. Disabled by default; purely a housekeeping extra, not required
    /// for correctness.</param>
    /// <param name="documentExpirationFrequency">How often the expiration sweep runs, once
    /// <paramref name="enableDocumentExpiration"/> is set. Defaults to 1 hour.</param>
    public static T UseRavenDb<T>(
        this T clusterConfigSelector,
        string connectionString,
        X509Certificate2? certificate = null,
        string? collectionPrefix = null,
        bool enableDocumentExpiration = false,
        TimeSpan? documentExpirationFrequency = null)
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

        if (documentExpirationFrequency.HasValue)
        {
            clusterConfigSelector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.DocumentExpirationFrequencyKey, (long)documentExpirationFrequency.Value.TotalSeconds);
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

    internal static TimeSpan GetDocumentExpirationFrequency(this JobMasterClusterConnectionConfig clusterConnectionConfig)
    {
        var seconds = clusterConnectionConfig.AdditionalConnConfig.TryGetValue<long?>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.DocumentExpirationFrequencyKey);
        return seconds.HasValue ? TimeSpan.FromSeconds(seconds.Value) : RavenDbConfigKeys.DefaultDocumentExpirationFrequency;
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
