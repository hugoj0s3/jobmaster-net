using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;

namespace JobMaster.RavenDb.Master;

// Static index for AcquireAndFetchAsync's candidate-selection query. The general Query/Count/
// ProbeForAcquireAsync surface stays on dynamic auto-indexing -- JobQueryCriteria has too many optional
// filter combinations (plus MetadataFilters, which no static index can cover) for full coverage.
//
// Naming convention: index name suffix = every mapped field, "_"-joined, in the Field* order below.
// RavenDbMasterJobsRepository.TryGetApplicableIndexName builds the same kind of string for a call's
// actual criteria and matches it as a StartsWith prefix -- extra fields an index maps beyond what's
// needed are harmless, so a shorter required prefix safely matches a longer index name.
internal static class RavenDbJobIndexes
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
