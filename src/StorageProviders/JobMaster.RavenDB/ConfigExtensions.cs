using System.Security.Cryptography.X509Certificates;
using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;

namespace JobMaster.RavenDB;

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
    /// <param name="requestTimeout">Overrides RavenDB.Client's own default HTTP request timeout for every
    /// operation against this connection. Left unset, RavenDB.Client's own default applies (a very
    /// generous 12-hour ceiling -- not a meaningful per-request timeout in practice).</param>
    /// <param name="pooledConnectionLifetime">Forces RavenDB.Client's underlying HTTP connection pool to
    /// proactively recycle a connection after this long, regardless of activity. Left unset, .NET's own
    /// <c>SocketsHttpHandler</c> default applies (<see cref="Timeout.InfiniteTimeSpan"/> -- connections are
    /// never proactively recycled), which can leave a pooled connection alive long enough for the server
    /// to have already silently closed its end, surfacing as a client-side "Connection reset by peer" on
    /// next use.</param>
    /// <param name="pooledConnectionIdleTimeout">Closes a pooled connection that's been idle this long.
    /// Left unset, .NET's own <c>SocketsHttpHandler</c> default applies.</param>
    public static T UseRavenDb<T>(
        this T clusterConfigSelector,
        string connectionString,
        X509Certificate2? certificate = null,
        string? collectionPrefix = null,
        bool enableDocumentExpiration = false,
        TimeSpan? documentExpirationFrequency = null,
        TimeSpan? requestTimeout = null,
        TimeSpan? pooledConnectionLifetime = null,
        TimeSpan? pooledConnectionIdleTimeout = null)
        where T : IBaseClusterConfigSelector<IClusterConfigSelector>
    {
        clusterConfigSelector.ClusterConnString(connectionString);
        clusterConfigSelector.ClusterRepoType(RavenDbRepositoryConstants.RepositoryTypeId);

        AppendConnectionOptions(
            (key, value) => clusterConfigSelector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, key, value),
            certificate,
            collectionPrefix,
            requestTimeout,
            pooledConnectionLifetime,
            pooledConnectionIdleTimeout);

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
    /// Configures an agent connection, or a hybrid transport/execution leg, to use RavenDB.
    /// See the cluster-side <c>UseRavenDb</c> for parameter details -- there's no document-expiration
    /// option here since agent messages never use <c>@expires</c>.
    /// </summary>
    public static T UseRavenDb<T>(
        this T agentConfigSelector,
        string connectionString,
        X509Certificate2? certificate = null,
        string? collectionPrefix = null,
        TimeSpan? requestTimeout = null,
        TimeSpan? pooledConnectionLifetime = null,
        TimeSpan? pooledConnectionIdleTimeout = null)
        where T : IAgentConnectionProviderSelector
    {
        agentConfigSelector.AgentConnString(connectionString);
        agentConfigSelector.AgentRepoType(RavenDbRepositoryConstants.RepositoryTypeId);

        AppendConnectionOptions(
            (key, value) => agentConfigSelector.AppendAdditionalConnConfigValue(RavenDbConfigKeys.NamespaceUniqueKey, key, value),
            certificate,
            collectionPrefix,
            requestTimeout,
            pooledConnectionLifetime,
            pooledConnectionIdleTimeout);

        return agentConfigSelector;
    }

    // Options shared by the cluster and agent UseRavenDb; the two selectors have no common Append interface.
    private static void AppendConnectionOptions(
        Action<string, object> append,
        X509Certificate2? certificate,
        string? collectionPrefix,
        TimeSpan? requestTimeout,
        TimeSpan? pooledConnectionLifetime,
        TimeSpan? pooledConnectionIdleTimeout)
    {
        if (certificate != null)
        {
            append(RavenDbConfigKeys.CertificateKey, certificate);
        }

        if (requestTimeout.HasValue)
        {
            append(RavenDbConfigKeys.RequestTimeoutKey, (long)requestTimeout.Value.TotalMilliseconds);
        }

        if (pooledConnectionLifetime.HasValue)
        {
            append(RavenDbConfigKeys.PooledConnectionLifetimeKey, (long)pooledConnectionLifetime.Value.TotalMilliseconds);
        }

        if (pooledConnectionIdleTimeout.HasValue)
        {
            append(RavenDbConfigKeys.PooledConnectionIdleTimeoutKey, (long)pooledConnectionIdleTimeout.Value.TotalMilliseconds);
        }

        if (collectionPrefix != null)
        {
            append(RavenDbConfigKeys.CollectionPrefixKey, collectionPrefix);
        }
    }

    // Shared by cluster (master) and agent provider configs.
    internal static string GetCollectionPrefix(this IHasAdditionalConnConfig connConfig)
    {
        return connConfig.AdditionalConnConfig.TryGetValue<string>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CollectionPrefixKey)
               ?? RavenDbConfigKeys.DefaultCollectionPrefix;
    }

    internal static X509Certificate2? GetCertificate(this IHasAdditionalConnConfig connConfig)
    {
        return connConfig.AdditionalConnConfig.TryGetValue<X509Certificate2>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.CertificateKey);
    }

    internal static TimeSpan? GetRequestTimeout(this IHasAdditionalConnConfig connConfig)
    {
        var ms = connConfig.AdditionalConnConfig.TryGetValue<long?>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.RequestTimeoutKey);
        return ms.HasValue ? TimeSpan.FromMilliseconds(ms.Value) : null;
    }

    internal static TimeSpan? GetPooledConnectionLifetime(this IHasAdditionalConnConfig connConfig)
    {
        var ms = connConfig.AdditionalConnConfig.TryGetValue<long?>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.PooledConnectionLifetimeKey);
        return ms.HasValue ? TimeSpan.FromMilliseconds(ms.Value) : null;
    }

    internal static TimeSpan? GetPooledConnectionIdleTimeout(this IHasAdditionalConnConfig connConfig)
    {
        var ms = connConfig.AdditionalConnConfig.TryGetValue<long?>(RavenDbConfigKeys.NamespaceUniqueKey, RavenDbConfigKeys.PooledConnectionIdleTimeoutKey);
        return ms.HasValue ? TimeSpan.FromMilliseconds(ms.Value) : null;
    }

    // Cluster-only: document expiration applies to the master database.
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
}
