using JobMaster.RavenDb.Connections;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc.Markups;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Json;

namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbMasterLogsRepository : JobMasterClusterAwareRepository, IMasterLogsRepository
{
    private readonly IDocumentStore store;

    public RavenDbMasterLogsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IRavenDbDocumentStoreManager storeManager) : base(clusterConnectionConfig)
    {
        store = storeManager.GetOrCreateStore(clusterConnectionConfig.ConnectionString, clusterConnectionConfig.GetCertificate(), clusterConnectionConfig.GetRequestTimeout(), clusterConnectionConfig.GetPooledConnectionLifetime(), clusterConnectionConfig.GetPooledConnectionIdleTimeout());
    }

    public override string MasterRepoTypeId => RavenDbRepositoryConstants.RepositoryTypeId;

    private string CollectionName => RavenDbDocumentNaming.CollectionName(ClusterConnConfig, RavenDbCollectionNames.Logs);

    private string DocumentId(Guid id) => RavenDbDocumentNaming.DocumentId(ClusterConnConfig, RavenDbCollectionNames.Logs, id.ToString("N"));

    public async Task BulkInsertAsync(IList<LogItem> items)
    {
        if (items.Count == 0) return;

        await using var bulkInsert = store.BulkInsert();
        foreach (var item in items)
        {
            var doc = ToDocument(item);
            var metadata = new MetadataAsDictionary { [Constants.Documents.Metadata.Collection] = CollectionName };
            await bulkInsert.StoreAsync(doc, DocumentId(item.Id), metadata);
        }
    }

    public async Task<List<LogItem>> QueryAsync(LogItemQueryCriteria criteria)
    {
        using var session = store.OpenAsyncSession();
        var (rql, parameters) = BuildQueryRql(criteria);

        // No WaitForNonStaleResults -- logs are pure history, nothing depends on reading the absolute
        // freshest snapshot the way GenericRecords' Query does for sentinel-gated cache refreshes.
        var query = session.Advanced.AsyncRawQuery<RavenDbLogDocument>(rql);
        foreach (var kv in parameters)
        {
            query = query.AddParameter(kv.Key, kv.Value);
        }

        var docs = await query.ToListAsync();
        return docs.Select(ToDomain).ToList();
    }

    public async Task<int> CountAsync(LogItemQueryCriteria criteria)
    {
        using var session = store.OpenAsyncSession();
        var (rql, parameters) = BuildQueryRql(criteria, forCount: true);

        var query = session.Advanced.AsyncRawQuery<RavenDbLogDocument>(rql).Statistics(out var stats);
        foreach (var kv in parameters)
        {
            query = query.AddParameter(kv.Key, kv.Value);
        }

        await query.ToListAsync();
        return (int)stats.TotalResults;
    }

    public async Task<LogItem?> GetAsync(Guid id)
    {
        using var session = store.OpenAsyncSession();
        var doc = await session.LoadAsync<RavenDbLogDocument>(DocumentId(id));
        return doc == null ? null : ToDomain(doc);
    }

    public async Task<int> DeleteByTimestampAsync(DateTime timestampTo, int limit, JobMasterLogCategory? excludeCategory = null)
    {
        if (limit <= 0) throw new ArgumentException("Limit must be greater than 0", nameof(limit));

        var cutoff = DateTime.SpecifyKind(timestampTo, DateTimeKind.Utc);

        // AllowStale is safe: a missed entry this tick is still just as old next tick.
        var excludeClause = excludeCategory.HasValue ? "and (e.Category = null or e.Category != $excludeCategory) " : "";
        var rql = $"from '{CollectionName}' as e where e.ClusterId = $clusterId and e.TimestampUtc <= $cutoff {excludeClause}order by e.TimestampUtc asc limit 0, $limit";
        var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery
        {
            Query = rql,
            QueryParameters = new Parameters
            {
                ["clusterId"] = ClusterConnConfig.ClusterId,
                ["cutoff"] = cutoff,
                ["limit"] = limit,
                ["excludeCategory"] = excludeCategory,
            }
        }, new QueryOperationOptions { AllowStale = true }));

        var result = await operation.WaitForCompletionAsync<BulkOperationResult>();
        return (int)result.Total;
    }

    public async Task<IList<LogItem>> QueryForReferenceIdsAsync(JobMasterLogCategory category, IList<string> referenceIds)
    {
        if (referenceIds.Count == 0) return new List<LogItem>();

        using var session = store.OpenAsyncSession();
        var rql = $"from '{CollectionName}' as e where e.ClusterId = $clusterId and e.Category = $category and e.ReferenceId in ($referenceIds)";
        var docs = await session.Advanced.AsyncRawQuery<RavenDbLogDocument>(rql)
            .AddParameter("clusterId", ClusterConnConfig.ClusterId)
            .AddParameter("category", category)
            .AddParameter("referenceIds", referenceIds)
            .ToListAsync();

        return docs.Select(ToDomain).ToList();
    }

    public async Task<int> DeleteByIdsAsync(IList<Guid> ids)
    {
        if (ids.Count == 0) return 0;

        var totalDeleted = 0;
        var maxBatchSize = OperationThrottlerSettingsTemplateFactory.GetMasterMaxBatchSize(ClusterConnConfig.RepositoryTypeId);
        for (var i = 0; i < ids.Count; i += maxBatchSize)
        {
            var batch = ids.Skip(i).Take(maxBatchSize).ToArray();

            using var session = store.OpenAsyncSession();
            foreach (var id in batch)
            {
                session.Delete(DocumentId(id));
            }

            await session.SaveChangesAsync();
            totalDeleted += batch.Length;
        }

        return totalDeleted;
    }

    private (string Rql, Dictionary<string, object?> Parameters) BuildQueryRql(LogItemQueryCriteria criteria, bool forCount = false)
    {
        var parameters = new Dictionary<string, object?> { ["clusterId"] = ClusterConnConfig.ClusterId };
        var where = new List<string> { "e.ClusterId = $clusterId" };

        if (criteria.Level.HasValue)
        {
            where.Add("e.Level = $level");
            parameters["level"] = criteria.Level.Value;
        }

        if (criteria.Category.HasValue)
        {
            where.Add("e.Category = $category");
            parameters["category"] = criteria.Category.Value;
        }

        if (!string.IsNullOrEmpty(criteria.ReferenceId))
        {
            where.Add("e.ReferenceId = $referenceId");
            parameters["referenceId"] = criteria.ReferenceId;
        }

        if (criteria.FromTimestamp.HasValue)
        {
            where.Add("e.TimestampUtc >= $fromTimestamp");
            parameters["fromTimestamp"] = DateTime.SpecifyKind(criteria.FromTimestamp.Value, DateTimeKind.Utc);
        }

        if (criteria.ToTimestamp.HasValue)
        {
            where.Add("e.TimestampUtc <= $toTimestamp");
            parameters["toTimestamp"] = DateTime.SpecifyKind(criteria.ToTimestamp.Value, DateTimeKind.Utc);
        }

        if (!string.IsNullOrEmpty(criteria.Keyword))
        {
            // No plain substring operator in RQL outside full-text search (token-based, not substring) --
            // regex() against an escaped literal gives exact substring semantics, matching GenericRecords'
            // Contains filter.
            where.Add("regex(e.Message, $keyword)");
            parameters["keyword"] = System.Text.RegularExpressions.Regex.Escape(criteria.Keyword);
        }

        var whereClause = "where " + string.Join(" and ", where);
        // No OrderBy field on LogItemQueryCriteria -- most-recent-first is the natural default for logs.
        const string orderByClause = "order by TimestampUtc desc";

        var limitClause = forCount
            ? "limit 0, 1" // Statistics().TotalResults reflects the true match count regardless of this cap.
            : $"limit {criteria.Offset}, {criteria.CountLimit}";

        var rql = $"from '{CollectionName}' as e {whereClause} {orderByClause} {limitClause}";
        return (rql, parameters);
    }

    private static RavenDbLogDocument ToDocument(LogItem item) => new()
    {
        LogId = item.Id,
        ClusterId = item.ClusterId,
        Level = item.Level,
        Message = item.Message,
        Category = item.Category,
        ReferenceId = item.ReferenceId,
        TimestampUtc = DateTime.SpecifyKind(item.TimestampUtc, DateTimeKind.Utc),
        Host = item.Host,
        SourceMember = item.SourceMember,
        SourceFile = item.SourceFile,
        SourceLine = item.SourceLine,
    };

    private static LogItem ToDomain(RavenDbLogDocument doc) => new()
    {
        Id = doc.LogId,
        ClusterId = doc.ClusterId,
        Level = doc.Level,
        Message = doc.Message,
        Category = doc.Category,
        ReferenceId = doc.ReferenceId,
        TimestampUtc = doc.TimestampUtc,
        Host = doc.Host,
        SourceMember = doc.SourceMember,
        SourceFile = doc.SourceFile,
        SourceLine = doc.SourceLine,
    };
}
