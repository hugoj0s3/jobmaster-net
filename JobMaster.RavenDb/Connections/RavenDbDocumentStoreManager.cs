using System.Collections.Concurrent;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace JobMaster.RavenDb.Connections;

internal sealed class RavenDbDocumentStoreManager : IRavenDbDocumentStoreManager, IDisposable
{
    private readonly ConcurrentDictionary<string, IDocumentStore> stores = new();

    public IDocumentStore GetOrCreateStore(string connectionString)
    {
        return stores.GetOrAdd(connectionString, static cnn =>
        {
            var (urls, database) = RavenDbConnectionStringParser.Parse(cnn);
            var store = new DocumentStore
            {
                Urls = urls,
                Database = database,
            };
            store.Initialize();
            return store;
        });
    }

    public IDocumentSession OpenSession(string connectionString) => GetOrCreateStore(connectionString).OpenSession();

    public IAsyncDocumentSession OpenAsyncSession(string connectionString) => GetOrCreateStore(connectionString).OpenAsyncSession();

    public void Dispose()
    {
        foreach (var store in stores.Values)
        {
            store.Dispose();
        }
        stores.Clear();
    }
}
