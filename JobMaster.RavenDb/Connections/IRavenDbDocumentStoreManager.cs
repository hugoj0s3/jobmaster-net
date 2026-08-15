using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace JobMaster.RavenDb.Connections;

/// <summary>
/// Caches one long-lived <see cref="IDocumentStore"/> per distinct connection string, since creating a
/// new <see cref="DocumentStore"/> per call is RavenDB's documented anti-pattern (unlike SQL's
/// open-per-call connection manager).
/// </summary>
internal interface IRavenDbDocumentStoreManager
{
    IDocumentStore GetOrCreateStore(string connectionString);

    IDocumentSession OpenSession(string connectionString);

    IAsyncDocumentSession OpenAsyncSession(string connectionString);
}
