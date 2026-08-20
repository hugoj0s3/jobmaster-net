using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;

namespace JobMaster.RavenDb.Master;

// Static index for AcquireAndFetchAsync's candidate-selection query specifically -- the one Jobs query
// this whole provider's performance work has centered on. The general Query/Count/ProbeForAcquireAsync
// surface stays on dynamic auto-indexing: JobQueryCriteria supports too many independent optional filter
// combinations (plus MetadataFilters, which structurally can never be covered by any static index, RQL
// has no bracket-index field syntax for arbitrary keys) to make full static-index coverage practical.
//
// Naming convention: the index name's suffix is every field it maps, joined by "_", in a fixed
// canonical order matching the Field* constants below. RavenDbMasterJobsRepository.TryGetApplicableIndexName
// builds the same kind of "_"-joined string for a specific call's actual criteria (only the fields that
// call needs, in this same order) and checks it as a prefix of a deployed index's name via StartsWith:
// extra fields an index maps beyond what a call needs are harmless (RavenDB doesn't require a query to
// reference every mapped field), so a shorter required prefix matching the start of a longer index name
// is a safe, valid match. If no deployed index's name starts with what a call needs, the caller falls
// back to `from '{CollectionName}'` -- unchanged dynamic-query behavior, same as if this didn't exist.
internal static class RavenDbJobIndexDefinitions
{
    public const string ClusterIdField = "ClusterId";
    public const string StatusField = "Status";
    public const string PartitionLockIdField = "PartitionLockId";
    public const string PartitionLockExpiresAtField = "PartitionLockExpiresAt";
    public const string NextPlanExecutionAtField = "NextPlanExecutionAt";

    private const string IndexNamePrefix = "RavenDbJobs/";

    public const string ByClusterStatusLockNextPlanName =
        IndexNamePrefix + ClusterIdField + "_" + StatusField + "_" + PartitionLockIdField + "_" + PartitionLockExpiresAtField + "_" + NextPlanExecutionAtField;

    // All currently-deployed job indexes -- TryGetApplicableIndexName checks a call's required prefix
    // against each of these. Just one for now; add more here (plus a DeployAsync call below) as more
    // AcquireAndFetchAsync-shaped criteria combinations turn out to matter.
    public static readonly IReadOnlyList<string> DeployedIndexNames = [ByClusterStatusLockNextPlanName];

    public static async Task DeployAsync(IDocumentStore store, string collectionName, CancellationToken ct = default)
    {
        var definition = new IndexDefinition
        {
            Name = ByClusterStatusLockNextPlanName,
            Maps =
            {
                $"from e in docs.{collectionName} select new {{ e.ClusterId, e.Status, e.PartitionLockId, e.PartitionLockExpiresAt, e.NextPlanExecutionAt }}",
            },
        };

        await store.Maintenance.SendAsync(new PutIndexesOperation(definition), ct);
    }
}
