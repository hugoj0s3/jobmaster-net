using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;

namespace JobMaster.RavenDb.Agents;

// Static index deployment for the Message collection -- one index covering both PullMessagesAsync's
// (BucketAddressId + ReferenceTime, filter+sort) and DestroyBucketAsync's (BucketAddressId alone) needs,
// so every message write pays indexing cost once instead of RavenDB creating and maintaining its own
// separate dynamic auto-index per distinct query shape it observes.
//
// Not a compile-time AbstractIndexCreationTask<RavenDbMessageDocument>: that base class infers its
// target collection from .NET type conventions, but this provider's collection names are
// prefix-configurable per connection (ConfigExtensions.UseRavenDb's collectionPrefix parameter), so the
// actual collection name is only known at deployment time, per connection -- the index definition is
// built with that resolved name instead.
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
