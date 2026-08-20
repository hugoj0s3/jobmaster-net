using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace JobMaster.RavenDb.Connections;

internal sealed class RavenDbDocumentStoreManager : IRavenDbDocumentStoreManager, IDisposable
{
    // Keyed by connection string + certificate thumbprint, not connection string alone -- two clusters
    // can share the same Urls/Database but authenticate with different client certificates, and a plain
    // connection-string key would incorrectly hand one of them the other's store.
    private readonly ConcurrentDictionary<(string ConnectionString, string? Thumbprint), IDocumentStore> stores = new();

    public IDocumentStore GetOrCreateStore(string connectionString, X509Certificate2? certificate = null)
    {
        var key = (connectionString, certificate?.Thumbprint);
        return stores.GetOrAdd(key, _ =>
        {
            var (urls, database) = RavenDbConnectionStringParser.Parse(connectionString);
            var store = new DocumentStore
            {
                Urls = urls,
                Database = database,
                Certificate = certificate,
            };
            store.Initialize();
            return store;
        });
    }

    public IDocumentSession OpenSession(string connectionString, X509Certificate2? certificate = null) =>
        GetOrCreateStore(connectionString, certificate).OpenSession();

    public IAsyncDocumentSession OpenAsyncSession(string connectionString, X509Certificate2? certificate = null) =>
        GetOrCreateStore(connectionString, certificate).OpenAsyncSession();

    public void Dispose()
    {
        foreach (var store in stores.Values)
        {
            store.Dispose();
        }
        stores.Clear();
    }
}
