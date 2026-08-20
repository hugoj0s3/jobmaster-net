using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace JobMaster.RavenDb.Connections;

/// <summary>
/// Caches one long-lived <see cref="IDocumentStore"/> per distinct connection string + certificate pair,
/// since creating a new <see cref="DocumentStore"/> per call is RavenDB's documented anti-pattern (unlike
/// SQL's open-per-call connection manager).
/// </summary>
internal interface IRavenDbDocumentStoreManager
{
    IDocumentStore GetOrCreateStore(string connectionString, X509Certificate2? certificate = null);

    IDocumentSession OpenSession(string connectionString, X509Certificate2? certificate = null);

    IAsyncDocumentSession OpenAsyncSession(string connectionString, X509Certificate2? certificate = null);
}
