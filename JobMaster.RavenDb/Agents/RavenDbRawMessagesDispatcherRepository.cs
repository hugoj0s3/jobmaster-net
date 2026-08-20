using JobMaster.RavenDb.Connections;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Json;

namespace JobMaster.RavenDb.Agents;

internal sealed class RavenDbRawMessagesDispatcherRepository : JobMasterClusterAwareComponent, IAgentRawMessagesDispatcherRepository
{
    // Exposed (not private) so RavenDbMessageIndexDefinitions/RavenDbJobMasterRuntimeSetup can build the
    // same prefix-qualified collection name for static-index deployment without duplicating this string.
    internal const string Collection = "Message";

    // See DestroyBucketAsync for why this needs AllowStale=false + a wait instead of the default fail-fast.
    private static readonly TimeSpan DestroyBucketStaleTimeout = TimeSpan.FromSeconds(15);

    private readonly IRavenDbDocumentStoreManager storeManager;
    private IDocumentStore store = null!;
    private string prefix = RavenDbConfigKeys.DefaultCollectionPrefix;

    public RavenDbRawMessagesDispatcherRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IRavenDbDocumentStoreManager storeManager) : base(clusterConnectionConfig)
    {
        this.storeManager = storeManager;
    }

    public string AgentRepoTypeId => RavenDbRepositoryConstants.RepositoryTypeId;

    public bool IsPollingBased => true;

    private string ClusterId => ClusterConnConfig.ClusterId;

    public void Initialize(JobMasterAgentConnectionConfig config)
    {
        store = storeManager.GetOrCreateStore(config.ConnectionString, config.GetCertificate());
        prefix = config.GetCollectionPrefix();
    }

    private string CollectionName => $"{prefix}{Collection}";

    // fullBucketAddressId is embedded as a raw ID segment, not re-validated -- it's always machine-built
    // by FullBucketAddressIdsUtil (e.g. "{bucketId}:Job-SavePending"), never free-form user input.
    private string BucketStreamPrefix(string fullBucketAddressId) =>
        RavenDbDocumentNaming.DocumentId(prefix, ClusterId, Collection, fullBucketAddressId) + "/";

    private string DocumentId(string fullBucketAddressId, string messageId) =>
        BucketStreamPrefix(fullBucketAddressId) + messageId;

    public string PushMessage(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId)
    {
        using var session = store.OpenSession();
        var (doc, docId) = BuildMessage(fullBucketAddressId, payload, referenceTime, correlationId);
        session.Store(doc, docId);
        ApplyCollectionMetadata(session.Advanced.GetMetadataFor(doc));
        session.SaveChanges();
        return doc.MessageId;
    }

    public async Task<string> PushMessageAsync(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId)
    {
        using var session = store.OpenAsyncSession();
        var (doc, docId) = BuildMessage(fullBucketAddressId, payload, referenceTime, correlationId);
        await session.StoreAsync(doc, docId);
        ApplyCollectionMetadata(session.Advanced.GetMetadataFor(doc));
        await session.SaveChangesAsync();
        return doc.MessageId;
    }

    public async Task<IList<string>> BulkPushMessageAsync(string fullBucketAddressId, IList<(string payload, DateTime referenceTime, string correlationId)> messages)
    {
        if (messages.Count == 0) return new List<string>();

        var ids = new List<string>(messages.Count);
        await using var bulkInsert = store.BulkInsert();
        foreach (var (payload, referenceTime, correlationId) in messages)
        {
            var (doc, docId) = BuildMessage(fullBucketAddressId, payload, referenceTime, correlationId);
            var metadata = new MetadataAsDictionary { [Constants.Documents.Metadata.Collection] = CollectionName };
            await bulkInsert.StoreAsync(doc, docId, metadata);
            ids.Add(doc.MessageId);
        }

        return ids;
    }

    public async Task<IList<JobMasterRawMessage>> PullMessagesAsync(string fullBucketAddressId, int numberOfJobs, DateTime? referenceTimeTo = null)
    {
        if (numberOfJobs <= 0) return new List<JobMasterRawMessage>();

        using var session = store.OpenAsyncSession();
        var where = "e.BucketAddressId = $bucket";
        if (referenceTimeTo.HasValue)
        {
            where += " and e.ReferenceTime <= $to";
        }

        // No WaitForNonStaleResults -- single-owner-per-bucket means no concurrent claimant to race
        // against, and a just-pushed message the index hasn't caught up to yet is simply picked up on the
        // next poll. LoadAsync below is a point lookup, so returned content is never stale either way.
        // Targets the static index shared with DestroyBucketAsync -- see RavenDbMessageIndexDefinitions.
        var rql = $"from index '{RavenDbMessageIndexDefinitions.ByBucketAndReferenceTimeName}' as e where {where} order by e.ReferenceTime asc select id() as Id limit 0, $limit";
        var query = session.Advanced.AsyncRawQuery<IdProjection>(rql)
            .AddParameter("bucket", fullBucketAddressId)
            .AddParameter("limit", numberOfJobs);
        if (referenceTimeTo.HasValue)
        {
            query = query.AddParameter("to", DateTime.SpecifyKind(referenceTimeTo.Value, DateTimeKind.Utc));
        }

        var ids = (await query.ToListAsync()).Select(p => p.Id).ToList();
        if (ids.Count == 0) return new List<JobMasterRawMessage>();

        var docs = await session.LoadAsync<RavenDbMessageDocument>(ids);

        var won = new List<JobMasterRawMessage>(ids.Count);
        foreach (var id in ids)
        {
            var doc = docs[id];
            // Already gone -- e.g. the index still listed an id from an earlier pull that already
            // deleted it. Self-correcting: just fewer results than requested this round.
            if (doc == null) continue;

            session.Delete(id);
            won.Add(ToDomain(doc));
        }

        await session.SaveChangesAsync();
        return won;
    }

    private sealed class IdProjection
    {
        public string Id { get; set; } = string.Empty;
    }

    public async Task<bool> HasJobsAsync(string fullBucketAddressId)
    {
        using var session = store.OpenAsyncSession();
        // Streams document IDs directly rather than querying -- bypasses indexes entirely, so this is
        // never affected by index staleness (unlike a Count-by-filter query would be).
        await using var stream = await session.Advanced.StreamAsync<RavenDbMessageDocument>(startsWith: BucketStreamPrefix(fullBucketAddressId));
        return await stream.MoveNextAsync();
    }

    public async Task CreateBucketAsync(string fullBucketAddressId)
    {
        // No-op -- unlike SQL's bucket_dispatcher registry row, RavenDB needs no pre-creation; pushing
        // the first message is sufficient.
        await Task.CompletedTask;
    }

    public async Task DestroyBucketAsync(string fullBucketAddressId)
    {
        // AllowStale=false + StaleTimeout wait, deliberately not fire-and-forget: unlike the retention
        // sweeps elsewhere (safe to under-delete since a missed row is still eligible next tick), this is
        // a one-shot teardown with no follow-up -- a message missed here due to index lag would be
        // orphaned forever, not just delayed. Same static index as PullMessagesAsync.
        var rql = $"from index '{RavenDbMessageIndexDefinitions.ByBucketAndReferenceTimeName}' as e where e.BucketAddressId = $bucket";
        var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery
        {
            Query = rql,
            QueryParameters = new Parameters
            {
                ["bucket"] = fullBucketAddressId,
            }
        }, new QueryOperationOptions { AllowStale = false, StaleTimeout = DestroyBucketStaleTimeout }));

        await operation.WaitForCompletionAsync();
    }

    private (RavenDbMessageDocument Doc, string DocId) BuildMessage(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId)
    {
        var messageId = JobMasterRandomUtil.NewGuid4().ToString("N");
        var doc = new RavenDbMessageDocument
        {
            MessageId = messageId,
            BucketAddressId = fullBucketAddressId,
            Payload = payload,
            ReferenceTime = DateTime.SpecifyKind(referenceTime, DateTimeKind.Utc),
            CorrelationId = correlationId,
            EnqueuedAt = DateTime.UtcNow,
        };
        return (doc, DocumentId(fullBucketAddressId, messageId));
    }

    private void ApplyCollectionMetadata(IMetadataDictionary metadata)
    {
        metadata[Constants.Documents.Metadata.Collection] = CollectionName;
    }

    private JobMasterRawMessage ToDomain(RavenDbMessageDocument doc) =>
        JobMasterRawMessage.RecoverFromDb(new JobMasterRawMessagePersistenceRecord
        {
            ClusterId = ClusterId,
            MessageId = doc.MessageId,
            Payload = doc.Payload,
            ReferenceTime = doc.ReferenceTime,
            CorrelationId = doc.CorrelationId,
            EnqueuedAt = doc.EnqueuedAt,
        });
}
