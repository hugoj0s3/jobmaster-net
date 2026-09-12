using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace JobMaster.RavenDb.Connections;

internal sealed class RavenDbDocumentStoreManager : IRavenDbDocumentStoreManager, IDisposable
{
    // Keyed by connection string + certificate thumbprint + every HTTP-behavior override, not connection
    // string alone -- two clusters can share the same Urls/Database but authenticate with different client
    // certificates (or want different timeouts), and a plain connection-string key would incorrectly hand
    // one of them the other's store.
    private readonly ConcurrentDictionary<(string ConnectionString, string? Thumbprint, TimeSpan? RequestTimeout, TimeSpan? PooledConnectionLifetime, TimeSpan? PooledConnectionIdleTimeout), IDocumentStore> stores = new();

    public IDocumentStore GetOrCreateStore(
        string connectionString,
        X509Certificate2? certificate = null,
        TimeSpan? requestTimeout = null,
        TimeSpan? pooledConnectionLifetime = null,
        TimeSpan? pooledConnectionIdleTimeout = null)
    {
        var key = (connectionString, certificate?.Thumbprint, requestTimeout, pooledConnectionLifetime, pooledConnectionIdleTimeout);
        return stores.GetOrAdd(key, _ =>
        {
            var (urls, database) = RavenDbConnectionStringParser.Parse(connectionString);
            var store = new DocumentStore
            {
                Urls = urls,
                Database = database,
                Certificate = certificate,
            };

            if (requestTimeout.HasValue)
            {
                store.Conventions.RequestTimeout = requestTimeout.Value;
            }

#if !NETSTANDARD2_0
            // HttpPooledConnectionLifetime/IdleTimeout map directly to SocketsHttpHandler, which doesn't
            // exist on .NET Standard 2.0 (it must also support .NET Framework) -- RavenDB.Client's
            // netstandard2.0 build omits these two conventions entirely for that reason. No fallback here;
            // on that TFM the pool simply uses .NET's own HttpClientHandler defaults, same as if these
            // parameters were never passed.
            if (pooledConnectionLifetime.HasValue)
            {
                store.Conventions.HttpPooledConnectionLifetime = pooledConnectionLifetime.Value;
            }

            if (pooledConnectionIdleTimeout.HasValue)
            {
                store.Conventions.HttpPooledConnectionIdleTimeout = pooledConnectionIdleTimeout.Value;
            }
#endif

            store.Initialize();
            return store;
        });
    }

    public IDocumentSession OpenSession(
        string connectionString,
        X509Certificate2? certificate = null,
        TimeSpan? requestTimeout = null,
        TimeSpan? pooledConnectionLifetime = null,
        TimeSpan? pooledConnectionIdleTimeout = null) =>
        GetOrCreateStore(connectionString, certificate, requestTimeout, pooledConnectionLifetime, pooledConnectionIdleTimeout).OpenSession();

    public IAsyncDocumentSession OpenAsyncSession(
        string connectionString,
        X509Certificate2? certificate = null,
        TimeSpan? requestTimeout = null,
        TimeSpan? pooledConnectionLifetime = null,
        TimeSpan? pooledConnectionIdleTimeout = null) =>
        GetOrCreateStore(connectionString, certificate, requestTimeout, pooledConnectionLifetime, pooledConnectionIdleTimeout).OpenAsyncSession();

    public void Dispose()
    {
        foreach (var store in stores.Values)
        {
            store.Dispose();
        }
        stores.Clear();
    }
}
