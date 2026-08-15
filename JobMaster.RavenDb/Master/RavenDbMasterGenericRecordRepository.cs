using JobMaster.Abstractions.Models;
using JobMaster.RavenDb.Connections;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc.Markups;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Json;

namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbMasterGenericRecordRepository : JobMasterClusterAwareRepository, IMasterGenericRecordRepository
{
    private readonly IDocumentStore store;

    public RavenDbMasterGenericRecordRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IRavenDbDocumentStoreManager storeManager) : base(clusterConnectionConfig)
    {
        store = storeManager.GetOrCreateStore(clusterConnectionConfig.ConnectionString);
    }

    public override string MasterRepoTypeId => RavenDbRepositoryConstants.RepositoryTypeId;

    private string CollectionName(string groupId) => RavenDbDocumentNaming.CollectionName(ClusterConnConfig, groupId);

    private string DocumentId(string groupId, string entryId) => RavenDbDocumentNaming.DocumentId(ClusterConnConfig, groupId, entryId);

    public GenericRecordEntry? Get(string groupId, string entryId, bool includeExpired = false)
    {
        using var session = store.OpenSession();
        var doc = session.Load<RavenDbGenericRecordDocument>(DocumentId(groupId, entryId));
        return ToDomainOrNull(doc, includeExpired);
    }

    public async Task<GenericRecordEntry?> GetAsync(string groupId, string entryId, bool includeExpired = false)
    {
        using var session = store.OpenAsyncSession();
        var doc = await session.LoadAsync<RavenDbGenericRecordDocument>(DocumentId(groupId, entryId));
        return ToDomainOrNull(doc, includeExpired);
    }

    private GenericRecordEntry? ToDomainOrNull(RavenDbGenericRecordDocument? doc, bool includeExpired)
    {
        if (doc == null) return null;
        if (!includeExpired && doc.ExpiresAt.HasValue && doc.ExpiresAt.Value <= DateTime.UtcNow) return null;
        return ToDomain(doc);
    }

    public IList<GenericRecordEntry> Query(string groupId, GenericRecordQueryCriteria? criteria = null)
    {
        criteria ??= new GenericRecordQueryCriteria();
        using var session = store.OpenSession();
        var (rql, parameters) = BuildQueryRql(groupId, criteria);

        var query = session.Advanced.RawQuery<RavenDbGenericRecordDocument>(rql);
        foreach (var kv in parameters)
        {
            query = query.AddParameter(kv.Key, kv.Value);
        }

        return query.ToList().Select(ToDomain).ToList();
    }

    public async Task<IList<GenericRecordEntry>> QueryAsync(string groupId, GenericRecordQueryCriteria? criteria = null)
    {
        criteria ??= new GenericRecordQueryCriteria();
        using var session = store.OpenAsyncSession();
        var (rql, parameters) = BuildQueryRql(groupId, criteria);

        var query = session.Advanced.AsyncRawQuery<RavenDbGenericRecordDocument>(rql);
        foreach (var kv in parameters)
        {
            query = query.AddParameter(kv.Key, kv.Value);
        }

        var docs = await query.ToListAsync();
        return docs.Select(ToDomain).ToList();
    }

    public void Upsert(GenericRecordEntry recordEntry)
    {
        using var session = store.OpenSession();
        var doc = ToDocument(recordEntry);
        session.Store(doc, DocumentId(recordEntry.GroupId, recordEntry.EntryId));
        session.Advanced.GetMetadataFor(doc)[Constants.Documents.Metadata.Collection] = CollectionName(recordEntry.GroupId);
        session.SaveChanges();
    }

    public async Task UpsertAsync(GenericRecordEntry recordEntry)
    {
        using var session = store.OpenAsyncSession();
        var doc = ToDocument(recordEntry);
        await session.StoreAsync(doc, DocumentId(recordEntry.GroupId, recordEntry.EntryId));
        session.Advanced.GetMetadataFor(doc)[Constants.Documents.Metadata.Collection] = CollectionName(recordEntry.GroupId);
        await session.SaveChangesAsync();
    }

    public void Insert(GenericRecordEntry recordEntry)
    {
        using var session = store.OpenSession();
        var doc = ToDocument(recordEntry);
        // Empty change vector is RavenDB's "must not already exist" convention -- throws
        // ConcurrencyException on a duplicate, left unmodified/unmapped since neither the interface
        // nor SQL's base Insert special-cases this at the repository layer.
        session.Store(doc, changeVector: string.Empty, id: DocumentId(recordEntry.GroupId, recordEntry.EntryId));
        session.Advanced.GetMetadataFor(doc)[Constants.Documents.Metadata.Collection] = CollectionName(recordEntry.GroupId);
        session.SaveChanges();
    }

    public async Task InsertAsync(GenericRecordEntry recordEntry)
    {
        using var session = store.OpenAsyncSession();
        var doc = ToDocument(recordEntry);
        await session.StoreAsync(doc, changeVector: string.Empty, id: DocumentId(recordEntry.GroupId, recordEntry.EntryId));
        session.Advanced.GetMetadataFor(doc)[Constants.Documents.Metadata.Collection] = CollectionName(recordEntry.GroupId);
        await session.SaveChangesAsync();
    }

    public void Delete(string groupId, string id)
    {
        using var session = store.OpenSession();
        session.Delete(DocumentId(groupId, id));
        session.SaveChanges();
    }

    public async Task DeleteAsync(string groupId, string id)
    {
        using var session = store.OpenAsyncSession();
        session.Delete(DocumentId(groupId, id));
        await session.SaveChangesAsync();
    }

    public async Task BulkInsertAsync(IList<GenericRecordEntry> records)
    {
        if (records.Count == 0) return;

        await using var bulkInsert = store.BulkInsert();
        foreach (var record in records)
        {
            var doc = ToDocument(record);
            var metadata = new MetadataAsDictionary { [Constants.Documents.Metadata.Collection] = CollectionName(record.GroupId) };
            await bulkInsert.StoreAsync(doc, DocumentId(record.GroupId, record.EntryId), metadata);
        }
    }

    public async Task<int> DeleteExpiredAsync(DateTime expiresAtTo, int limit)
    {
        if (limit <= 0) throw new ArgumentException("Limit must be greater than 0", nameof(limit));

        var cutoff = DateTime.SpecifyKind(expiresAtTo, DateTimeKind.Utc);

        // No groupId parameter here -- this purges across every group, including groups this provider
        // has no static knowledge of (arbitrary caller-supplied GroupIds). Collections are discovered
        // dynamically rather than iterating a fixed MasterGenericRecordGroupIds list, since that list
        // only covers the framework's own well-known groups.
        var collectionNames = await DiscoverPrefixedCollectionsAsync();

        var totalDeleted = 0;
        foreach (var collectionName in collectionNames)
        {
            using var session = store.OpenAsyncSession();
            var rql = $"from '{collectionName}' as e where e.ClusterId = $clusterId and e.ExpiresAt != null and e.ExpiresAt <= $cutoff order by e.ExpiresAt asc limit 0, $limit";
            var docs = await session.Advanced.AsyncRawQuery<RavenDbGenericRecordDocument>(rql)
                .AddParameter("clusterId", ClusterConnConfig.ClusterId)
                .AddParameter("cutoff", cutoff)
                .AddParameter("limit", limit)
                .ToListAsync();

            if (docs.Count == 0) continue;

            foreach (var doc in docs)
            {
                session.Delete(session.Advanced.GetDocumentId(doc));
            }
            await session.SaveChangesAsync();
            totalDeleted += docs.Count;
        }

        return totalDeleted;
    }

    public async Task<int> DeleteByCreatedAtAsync(string groupId, DateTime createdAtTo, int limit)
    {
        if (string.IsNullOrEmpty(groupId)) throw new ArgumentException("groupId is required", nameof(groupId));
        if (limit <= 0) throw new ArgumentException("Limit must be greater than 0", nameof(limit));

        var cutoff = DateTime.SpecifyKind(createdAtTo, DateTimeKind.Utc);

        using var session = store.OpenAsyncSession();
        var rql = $"from '{CollectionName(groupId)}' as e where e.ClusterId = $clusterId and e.CreatedAt <= $cutoff order by e.CreatedAt asc limit 0, $limit";
        var docs = await session.Advanced.AsyncRawQuery<RavenDbGenericRecordDocument>(rql)
            .AddParameter("clusterId", ClusterConnConfig.ClusterId)
            .AddParameter("cutoff", cutoff)
            .AddParameter("limit", limit)
            .ToListAsync();

        if (docs.Count == 0) return 0;

        foreach (var doc in docs)
        {
            session.Delete(session.Advanced.GetDocumentId(doc));
        }
        await session.SaveChangesAsync();
        return docs.Count;
    }

    public int Count(string groupId, GenericRecordQueryCriteria? criteria = null)
    {
        criteria ??= new GenericRecordQueryCriteria();
        using var session = store.OpenSession();
        var (rql, parameters) = BuildQueryRql(groupId, criteria, forCount: true);

        var query = session.Advanced.RawQuery<RavenDbGenericRecordDocument>(rql).Statistics(out var stats);
        foreach (var kv in parameters)
        {
            query = query.AddParameter(kv.Key, kv.Value);
        }
        query.ToList();
        return (int)stats.TotalResults;
    }

    public async Task<int> CountAsync(string groupId, GenericRecordQueryCriteria? criteria = null)
    {
        criteria ??= new GenericRecordQueryCriteria();
        using var session = store.OpenAsyncSession();
        var (rql, parameters) = BuildQueryRql(groupId, criteria, forCount: true);

        var query = session.Advanced.AsyncRawQuery<RavenDbGenericRecordDocument>(rql).Statistics(out var stats);
        foreach (var kv in parameters)
        {
            query = query.AddParameter(kv.Key, kv.Value);
        }
        await query.ToListAsync();
        return (int)stats.TotalResults;
    }

    private async Task<IReadOnlyList<string>> DiscoverPrefixedCollectionsAsync()
    {
        var prefix = ClusterConnConfig.GetCollectionPrefix();
        var stats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
        return stats.Collections.Keys
            .Where(c => c.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            .ToList();
    }

    private (string Rql, Dictionary<string, object?> Parameters) BuildQueryRql(string groupId, GenericRecordQueryCriteria criteria, bool forCount = false)
    {
        var parameters = new Dictionary<string, object?>();
        var where = new List<string> { "e.ClusterId = $clusterId" };
        parameters["clusterId"] = ClusterConnConfig.ClusterId;

        if (!criteria.IncludeExpired)
        {
            where.Add("(e.ExpiresAt = null or e.ExpiresAt > $now)");
            parameters["now"] = DateTime.UtcNow;
        }

        if (criteria.EntryIds.Count > 0)
        {
            where.Add("e.EntryId in ($entryIds)");
            parameters["entryIds"] = criteria.EntryIds.ToArray();
        }

        if (criteria.CreatedAtFrom.HasValue)
        {
            where.Add("e.CreatedAt >= $createdAtFrom");
            parameters["createdAtFrom"] = DateTime.SpecifyKind(criteria.CreatedAtFrom.Value, DateTimeKind.Utc);
        }

        if (criteria.CreatedAtTo.HasValue)
        {
            where.Add("e.CreatedAt <= $createdAtTo");
            parameters["createdAtTo"] = DateTime.SpecifyKind(criteria.CreatedAtTo.Value, DateTimeKind.Utc);
        }

        if (criteria.ExpiresAtFrom.HasValue)
        {
            where.Add("(e.ExpiresAt != null and e.ExpiresAt >= $expiresAtFrom)");
            parameters["expiresAtFrom"] = DateTime.SpecifyKind(criteria.ExpiresAtFrom.Value, DateTimeKind.Utc);
        }

        if (criteria.ExpiresAtTo.HasValue)
        {
            where.Add("(e.ExpiresAt != null and e.ExpiresAt <= $expiresAtTo)");
            parameters["expiresAtTo"] = DateTime.SpecifyKind(criteria.ExpiresAtTo.Value, DateTimeKind.Utc);
        }

        for (var i = 0; i < criteria.Filters.Count; i++)
        {
            var (clause, paramName, paramValue) = BuildFilterClause(criteria.Filters[i], i);
            where.Add(clause);
            if (paramName != null)
            {
                parameters[paramName] = paramValue;
            }
        }

        var whereClause = "where " + string.Join(" and ", where);
        var orderByClause = criteria.OrderBy switch
        {
            GenericRecordQueryOrderByTypeId.CreatedAtAsc => "order by CreatedAt asc",
            GenericRecordQueryOrderByTypeId.CreatedAtDesc => "order by CreatedAt desc",
            GenericRecordQueryOrderByTypeId.ExpiresAtAsc => "order by ExpiresAt asc",
            GenericRecordQueryOrderByTypeId.ExpiresAtDesc => "order by ExpiresAt desc",
            _ => "order by CreatedAt desc"
        };

        string limitClause;
        if (forCount)
        {
            // Statistics().TotalResults reflects the true match count regardless of this cap -- keep the
            // actual document fetch tiny since only the count is needed.
            limitClause = "limit 0, 1";
        }
        else if (criteria.Limit.HasValue)
        {
            limitClause = $"limit {criteria.Offset ?? 0}, {criteria.Limit.Value}";
        }
        else
        {
            limitClause = string.Empty;
        }

        var rql = $"from '{CollectionName(groupId)}' as e {whereClause} {orderByClause} {limitClause}";
        return (rql, parameters);
    }

    private static (string Clause, string? ParamName, object? ParamValue) BuildFilterClause(GenericRecordValueFilter filter, int index)
    {
        // RQL has no bracket-index syntax for nested-object field access (`Values['key']` is a parse
        // error -- RQL only understands dot-path field access), so this only works for keys that are
        // valid identifiers; caller-supplied keys with spaces/hyphens/leading digits aren't queryable
        // this way -- a known limitation, not attempted here.
        //
        // Which bucket dictionary a key lives in (Values vs DateTimeValues/GuidValues/DecimalValues) is
        // determined by the FILTER's own value type, mirroring exactly how the write path (ToDocument)
        // dispatches by the stored value's runtime type -- the two must always agree.
        var sampleValue = filter.Operation == GenericFilterOperation.In
            ? filter.Values?.FirstOrDefault(v => v != null)
            : filter.Value;
        var bucket = sampleValue switch
        {
            DateTime => "DateTimeValues",
            Guid => "GuidValues",
            decimal => "DecimalValues",
            _ => "Values"
        };
        var field = $"e.{bucket}.{filter.Key}";
        var paramName = $"filterValue{index}";

        switch (filter.Operation)
        {
            case GenericFilterOperation.Eq:
                return ($"{field} = ${paramName}", paramName, filter.Value);
            case GenericFilterOperation.Neq:
                // Must also match documents that don't have this key at all -- a missing field access
                // in RQL yields null/undefined, and null != a non-null value is true, same as JS.
                return ($"{field} != ${paramName}", paramName, filter.Value);
            case GenericFilterOperation.In:
                return ($"{field} in (${paramName})", paramName, filter.Values?.ToArray() ?? Array.Empty<object?>());
            case GenericFilterOperation.Gt:
                return ($"{field} > ${paramName}", paramName, filter.Value);
            case GenericFilterOperation.Gte:
                return ($"{field} >= ${paramName}", paramName, filter.Value);
            case GenericFilterOperation.Lt:
                return ($"{field} < ${paramName}", paramName, filter.Value);
            case GenericFilterOperation.Lte:
                return ($"{field} <= ${paramName}", paramName, filter.Value);
            case GenericFilterOperation.Contains:
                // No plain substring operator in RQL outside full-text search (which is token-based, not
                // substring-based) -- regex() against an escaped literal gives exact substring semantics.
                return ($"regex({field}, ${paramName})", paramName, System.Text.RegularExpressions.Regex.Escape(filter.Value?.ToString() ?? string.Empty));
            case GenericFilterOperation.StartsWith:
                return ($"startsWith({field}, ${paramName})", paramName, filter.Value?.ToString());
            case GenericFilterOperation.EndsWith:
                return ($"endsWith({field}, ${paramName})", paramName, filter.Value?.ToString());
            default:
                throw new NotSupportedException($"Unsupported filter operation: {filter.Operation}");
        }
    }

    private static RavenDbGenericRecordDocument ToDocument(GenericRecordEntry entry)
    {
        var doc = new RavenDbGenericRecordDocument
        {
            ClusterId = entry.ClusterId,
            GroupId = entry.GroupId,
            EntryId = entry.EntryId,
            CreatedAt = DateTime.SpecifyKind(entry.CreatedAt, DateTimeKind.Utc),
            ExpiresAt = entry.ExpiresAt.HasValue ? DateTime.SpecifyKind(entry.ExpiresAt.Value, DateTimeKind.Utc) : null,
        };

        foreach (var kv in entry.Values)
        {
            switch (kv.Value)
            {
                case DateTime dt:
                    doc.DateTimeValues[kv.Key] = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                    break;
                case Guid g:
                    doc.GuidValues[kv.Key] = g;
                    break;
                case decimal dec:
                    doc.DecimalValues[kv.Key] = dec;
                    break;
                default:
                    doc.Values[kv.Key] = kv.Value;
                    break;
            }
        }

        return doc;
    }

    private static GenericRecordEntry ToDomain(RavenDbGenericRecordDocument doc)
    {
        var merged = new Dictionary<string, object?>(doc.Values, StringComparer.Ordinal);
        foreach (var kv in doc.DateTimeValues) merged[kv.Key] = kv.Value;
        foreach (var kv in doc.GuidValues) merged[kv.Key] = kv.Value;
        foreach (var kv in doc.DecimalValues) merged[kv.Key] = kv.Value;

        var entry = GenericRecordEntry.FromWritableMetadata(
            doc.ClusterId,
            doc.GroupId,
            doc.EntryId,
            new Metadata(merged),
            expiresAt: doc.ExpiresAt);
        entry.CreatedAt = doc.CreatedAt;
        return entry;
    }
}
