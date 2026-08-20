using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;

namespace JobMaster.RavenDb.Agents;

// One static index covering both PullMessagesAsync (BucketAddressId + ReferenceTime) and
// DestroyBucketAsync (BucketAddressId alone), instead of RavenDB maintaining a separate dynamic
// auto-index per query shape. Not a compile-time AbstractIndexCreationTask<T>: that infers its collection
// from .NET type conventions, but collection names here are prefix-configurable per connection, so the
// name is only known at deployment time.
internal static class RavenDbMessageIndexDefinitions
{
    public const string ByBucketAndReferenceTimeName = "RavenDbMessages/ByBucketAndReferenceTime";

    public static async Task DeployAsync(IDocumentStore store, string collectionName, CancellationToken ct = default)
    {
        var definition = new IndexDefinition
        {
            Name = ByBucketAndReferenceTimeName,
            Maps =
            {
                $"from e in docs.{collectionName} select new {{ e.BucketAddressId, e.ReferenceTime }}",
            },
        };

        await store.Maintenance.SendAsync(new PutIndexesOperation(definition), ct);
    }
}
