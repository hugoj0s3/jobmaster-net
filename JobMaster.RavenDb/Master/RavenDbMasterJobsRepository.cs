using System.Linq.Expressions;
using JobMaster.Abstractions.Models;
using JobMaster.RavenDb.Connections;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Ioc.Markups;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;

namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbMasterJobsRepository : JobMasterClusterAwareRepository, IMasterJobsRepository
{
    private const string Collection = "Job";
    private const string ExecutionCollection = "JobExecution";

    // Deliberately separate from JobMasterConstants.MaxBatchSizeForBulkOperation (50) -- that constant
    // bounds how many ids go in one server-side batch/patch call, not how many concurrent RavenDB
    // sessions/connections this repository opens at once. 50 concurrent sessions per bulk call is too
    // many; this only governs the per-row-session fallback paths (BulkUpdateAsync's per-row retry,
    // BulkInsertIfNotExistsAsync).
    private const int PerRowSessionMaxDegreeOfParallelism = 10;

    // Only AcquireAndFetchAsync's confirmatory read-back uses this -- it's the one read where staleness
    // would silently strand a job the caller actually won (patch succeeded server-side, but the caller
    // never finds out, so the job sits locked and unprocessed until the lease expires). Every other read
    // in this repository is either self-correcting via repeated polling/claiming or fine with SQL-equivalent
    // staleness, so this constant intentionally isn't reused elsewhere. 10s, not the 2s used elsewhere in
    // this provider -- claim contention can be heavier than a single message-queue bucket, and missing a
    // just-won job here is a worse outcome than the read taking a bit longer.
    private static readonly TimeSpan AcquireConfirmationNonStaleTimeout = TimeSpan.FromSeconds(10);
    
    private static readonly JobMasterJobStatus[] FinalStatuses =
    {
        JobMasterJobStatus.Succeeded, JobMasterJobStatus.Failed, JobMasterJobStatus.Cancelled, JobMasterJobStatus.Aborted,
    };

    private readonly IDocumentStore store;
    private readonly IJobMasterLogger logger;

    public RavenDbMasterJobsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IRavenDbDocumentStoreManager storeManager,
        IJobMasterLogger logger) : base(clusterConnectionConfig)
    {
        store = storeManager.GetOrCreateStore(clusterConnectionConfig.ConnectionString);
        this.logger = logger;
    }

    public override string MasterRepoTypeId => RavenDbRepositoryConstants.RepositoryTypeId;

    private string CollectionName => RavenDbDocumentNaming.CollectionName(ClusterConnConfig, Collection);
    private string ExecutionCollectionName => RavenDbDocumentNaming.CollectionName(ClusterConnConfig, ExecutionCollection);
    private string DocumentId(Guid jobId) => RavenDbDocumentNaming.DocumentId(ClusterConnConfig, Collection, jobId.ToString("N"));
    private string ExecutionDocumentId(Guid executionId) => RavenDbDocumentNaming.DocumentId(ClusterConnConfig, ExecutionCollection, executionId.ToString("N"));

    // ==================== Add ====================

    public void Add(JobRawModel jobRaw)
    {
        using var session = store.OpenSession();
        var doc = ToDocument(jobRaw);
        try
        {
            // Empty change vector is RavenDB's "must not already exist" convention.
            session.Store(doc, changeVector: string.Empty, id: DocumentId(jobRaw.Id));
            ApplyJobCollectionMetadata(session.Advanced.GetMetadataFor(doc));
            session.SaveChanges();
        }
        catch (ConcurrencyException ex)
        {
            throw new JobMasterDuplicationException(jobRaw.Id, "Job", ex);
        }

        jobRaw.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
    }

    public async Task AddAsync(JobRawModel jobRaw)
    {
        using var session = store.OpenAsyncSession();
        var doc = ToDocument(jobRaw);
        try
        {
            await session.StoreAsync(doc, changeVector: string.Empty, id: DocumentId(jobRaw.Id));
            ApplyJobCollectionMetadata(session.Advanced.GetMetadataFor(doc));
            await session.SaveChangesAsync();
        }
        catch (ConcurrencyException ex)
        {
            throw new JobMasterDuplicationException(jobRaw.Id, "Job", ex);
        }

        jobRaw.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
    }

    // ==================== Update ====================

    public void Update(JobRawModel jobRaw, JobExecution? addJobExecution = null)
    {
        using var session = store.OpenSession();
        var existing = session.Load<RavenDbJobDocument>(DocumentId(jobRaw.Id));
        if (existing == null) return; // Matches SQL: updating a job that doesn't exist at all is a silent no-op.

        // Evict so the CAS Store below (a fresh object, same id) isn't rejected as "already tracked".
        session.Advanced.Evict(existing);

        var doc = ToDocument(jobRaw);
        PreserveMetadata(doc, existing);

        try
        {
            session.Store(doc, changeVector: jobRaw.Version ?? string.Empty, id: DocumentId(jobRaw.Id));
            ApplyJobCollectionMetadata(session.Advanced.GetMetadataFor(doc));
            if (addJobExecution != null)
            {
                var execDoc = ToExecutionDocument(addJobExecution);
                session.Store(execDoc, ExecutionDocumentId(addJobExecution.Id));
                ApplyExecutionCollectionMetadata(session.Advanced.GetMetadataFor(execDoc));
            }

            session.SaveChanges();
        }
        catch (ConcurrencyException)
        {
            throw new JobMasterVersionConflictException(jobRaw.Id, "Job", jobRaw.Version);
        }

        jobRaw.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
    }

    public async Task UpdateAsync(JobRawModel jobRaw, JobExecution? addJobExecution = null)
    {
        using var session = store.OpenAsyncSession();
        var existing = await session.LoadAsync<RavenDbJobDocument>(DocumentId(jobRaw.Id));
        if (existing == null) return;

        session.Advanced.Evict(existing);

        var doc = ToDocument(jobRaw);
        PreserveMetadata(doc, existing);

        try
        {
            await session.StoreAsync(doc, changeVector: jobRaw.Version ?? string.Empty, id: DocumentId(jobRaw.Id));
            ApplyJobCollectionMetadata(session.Advanced.GetMetadataFor(doc));
            if (addJobExecution != null)
            {
                var execDoc = ToExecutionDocument(addJobExecution);
                await session.StoreAsync(execDoc, ExecutionDocumentId(addJobExecution.Id));
                ApplyExecutionCollectionMetadata(session.Advanced.GetMetadataFor(execDoc));
            }

            await session.SaveChangesAsync();
        }
        catch (ConcurrencyException)
        {
            throw new JobMasterVersionConflictException(jobRaw.Id, "Job", jobRaw.Version);
        }

        jobRaw.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
    }

    // Metadata is immutable after insert (conformance-verified) -- Update/BulkUpdate(per-row) must never
    // let the incoming model's Metadata (which callers typically don't even populate for an update)
    // overwrite what's already persisted.
    private static void PreserveMetadata(RavenDbJobDocument target, RavenDbJobDocument existing)
    {
        target.MetadataJson = existing.MetadataJson;
        target.MetadataValues = existing.MetadataValues;
        target.MetadataDateTimeValues = existing.MetadataDateTimeValues;
        target.MetadataGuidValues = existing.MetadataGuidValues;
        target.MetadataDecimalValues = existing.MetadataDecimalValues;
    }

    // ==================== JobExecution ====================

    public async Task AddJobExecutionAsync(JobExecution jobExecution)
    {
        using var session = store.OpenAsyncSession();
        var doc = ToExecutionDocument(jobExecution);
        await session.StoreAsync(doc, ExecutionDocumentId(jobExecution.Id));
        ApplyExecutionCollectionMetadata(session.Advanced.GetMetadataFor(doc));
        await session.SaveChangesAsync();
    }

    public async Task<IList<JobExecution>> QueryJobExecutionsAsync(Guid jobId)
    {
        using var session = store.OpenAsyncSession();
        var rql = $"from '{ExecutionCollectionName}' as e where e.ClusterId = $clusterId and e.JobId = $jobId order by e.StartedAt desc";
        var docs = await session.Advanced.AsyncRawQuery<RavenDbJobExecutionDocument>(rql)
            .AddParameter("clusterId", ClusterConnConfig.ClusterId)
            .AddParameter("jobId", jobId)
            .ToListAsync();

        return docs.Select(ToDomain).ToList();
    }

    // ==================== Exists ====================

    public bool Exists(Guid jobId)
    {
        using var session = store.OpenSession();
        return session.Advanced.Exists(DocumentId(jobId));
    }

    public async Task<bool> ExistsAsync(Guid jobId)
    {
        using var session = store.OpenAsyncSession();
        return await session.Advanced.ExistsAsync(DocumentId(jobId));
    }

    // ==================== Get ====================

    public JobRawModel? Get(Guid jobId)
    {
        using var session = store.OpenSession();
        var doc = session.Load<RavenDbJobDocument>(DocumentId(jobId));
        return doc == null ? null : ToDomain(doc, session.Advanced.GetChangeVectorFor(doc));
    }

    public async Task<JobRawModel?> GetAsync(Guid jobId)
    {
        using var session = store.OpenAsyncSession();
        var doc = await session.LoadAsync<RavenDbJobDocument>(DocumentId(jobId));
        return doc == null ? null : ToDomain(doc, session.Advanced.GetChangeVectorFor(doc));
    }
    
    public IList<JobRawModel> Query(JobQueryCriteria queryCriteria)
    {
        using var session = store.OpenSession();
        var (rql, parameters) = BuildQueryRql(queryCriteria);
        var query = session.Advanced.RawQuery<RavenDbJobDocument>(rql);
        foreach (var kv in parameters) query = query.AddParameter(kv.Key, kv.Value);

        var docs = query.ToList();
        return docs.Select(doc => ToDomain(doc, session.Advanced.GetChangeVectorFor(doc))).ToList();
    }

    public async Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria)
    {
        using var session = store.OpenAsyncSession();
        var (rql, parameters) = BuildQueryRql(queryCriteria);
        var query = session.Advanced.AsyncRawQuery<RavenDbJobDocument>(rql);
        foreach (var kv in parameters) query = query.AddParameter(kv.Key, kv.Value);

        var docs = await query.ToListAsync();
        return docs.Select(doc => ToDomain(doc, session.Advanced.GetChangeVectorFor(doc))).ToList();
    }

    public long Count(JobQueryCriteria queryCriteria)
    {
        using var session = store.OpenSession();
        var (where, parameters) = BuildWhereRql(queryCriteria, isLocked: null);
        var rql = $"from '{CollectionName}' as e where {where} limit 0, 1";
        var query = session.Advanced.RawQuery<RavenDbJobDocument>(rql).Statistics(out var stats);
        foreach (var kv in parameters) query = query.AddParameter(kv.Key, kv.Value);
        query.ToList();
        return stats.TotalResults;
    }

    private (string Rql, Dictionary<string, object?> Parameters) BuildQueryRql(JobQueryCriteria criteria)
    {
        var (where, parameters) = BuildWhereRql(criteria, isLocked: null);
        var orderBy = criteria.SortBy != null
            ? $"order by e.{MapSortProperty(criteria.SortBy.Property)} {(criteria.SortBy.Ascending ? "asc" : "desc")}"
            : "order by e.NextPlanExecutionAt asc";
        parameters["offset"] = criteria.Offset;
        parameters["limit"] = criteria.CountLimit;
        var rql = $"from '{CollectionName}' as e where {where} {orderBy} limit $offset, $limit";
        return (rql, parameters);
    }

    private static string MapSortProperty(string property) => property == nameof(JobRawModel.Id) ? "JobId" : property;

    // ==================== ProbeForAcquireAsync ====================

    public async Task<JobProbeResult> ProbeForAcquireAsync(JobQueryCriteria queryCriteria)
    {
        if (queryCriteria.MetadataFilters.Count > 0)
        {
            throw new NotSupportedException("MetadataFilters are not supported by ProbeForAcquireAsync.");
        }

        // Not a group-by aggregate -- RavenDB's dynamic group-by only supports count()/sum() as
        // aggregation methods (min() throws "Unknown aggregation method in SELECT clause of the group by
        // query"), and even count()-only group-by requires WHERE to come AFTER group by, which only lets
        // it filter on group-key/aggregate fields -- unusable for our arbitrary pre-aggregation criteria
        // filters.
        //
        // One round trip in the common case, two only when needed: NextPlanExecutionAt is nullable
        // (cleared on finalization), and RavenDB sorts nulls FIRST ascending -- unlike SQL's MIN(), which
        // silently ignores NULLs. So the first query orders by
        // NextPlanExecutionAt asc and takes row 0 alongside Statistics() for Count. If row 0's
        // NextPlanExecutionAt isn't null, that's already the true min -- nulls sorting first means no row
        // could have a smaller sort key, so no null exists anywhere in the result set. Only when row 0 is
        // null (or there are no rows at all) does a second query, adding "NextPlanExecutionAt != null" to
        // the same predicate, run to find the true min among any remaining non-null rows.
        using var session = store.OpenAsyncSession();
        var (where, parameters) = BuildWhereRql(queryCriteria, isLocked: false);

        var rql = $"from '{CollectionName}' as e where {where} order by e.NextPlanExecutionAt asc limit 0, 1";
        var query = session.Advanced.AsyncRawQuery<RavenDbJobDocument>(rql).Statistics(out var stats);
        foreach (var kv in parameters) query = query.AddParameter(kv.Key, kv.Value);
        var results = await query.ToListAsync();

        DateTime? min;
        if (results.Count > 0 && results[0].NextPlanExecutionAt != null)
        {
            min = results[0].NextPlanExecutionAt;
        }
        else if (results.Count == 0)
        {
            min = null;
        }
        else
        {
            var minRql = $"from '{CollectionName}' as e where {where} and e.NextPlanExecutionAt != null order by e.NextPlanExecutionAt asc limit 0, 1";
            var minQuery = session.Advanced.AsyncRawQuery<RavenDbJobDocument>(minRql);
            foreach (var kv in parameters) minQuery = minQuery.AddParameter(kv.Key, kv.Value);
            var minResults = await minQuery.ToListAsync();
            min = minResults.Count > 0 ? minResults[0].NextPlanExecutionAt : null;
        }

        return new JobProbeResult
        {
            Count = stats.TotalResults,
            MinNextPlanExecutionAt = min,
        };
    }

    private sealed class IdProjection
    {
        public string Id { get; set; } = string.Empty;
    }

    private sealed class JobIdProjection
    {
        public Guid JobId { get; set; }
    }

    // ==================== AcquireAndFetchAsync ====================

    public async Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, Guid partitionLockId, DateTime expiresAtUtc)
    {
        if (partitionLockId == Guid.Empty) throw new ArgumentException("partitionLockId must not be empty", nameof(partitionLockId));

        var lockNow = JobMasterConstants.NowUtcWithSkewTolerance();
        var lockExpiresAt = DateTime.SpecifyKind(expiresAtUtc, DateTimeKind.Utc);

        // Step 1: candidate selection -- ordered/limited/filtered, index-backed, can be stale. No
        // WaitForNonStaleResults here: this only decides which ids get a claim ATTEMPT, not who actually
        // wins one, and AcquireAndFetchAsync is itself called repeatedly by a polling claim loop, so a
        // candidate missed this round due to lag is picked up next round -- self-correcting, same
        // reasoning as everywhere else staleness got dropped in this provider.
        using var candidateSession = store.OpenAsyncSession();
        var (where, parameters) = BuildWhereRql(queryCriteria, isLocked: false);
        var orderBy = queryCriteria.SortBy != null
            ? $"order by e.{MapSortProperty(queryCriteria.SortBy.Property)} {(queryCriteria.SortBy.Ascending ? "asc" : "desc")}"
            : "order by e.NextPlanExecutionAt asc";
        parameters["offset"] = queryCriteria.Offset;
        parameters["limit"] = queryCriteria.CountLimit;
        var candidateRql = $"from '{CollectionName}' as e where {where} {orderBy} select id() as Id limit $offset, $limit";
        var candidateQuery = candidateSession.Advanced.AsyncRawQuery<IdProjection>(candidateRql);
        foreach (var kv in parameters) candidateQuery = candidateQuery.AddParameter(kv.Key, kv.Value);
        var candidateIds = (await candidateQuery.ToListAsync()).Select(p => p.Id).ToArray();

        if (candidateIds.Length == 0) return new List<JobRawModel>();

        // Step 2: server-side patch, one round trip covering every candidate. The "still unlocked" check
        // is repeated INSIDE the update script (not just in the outer where), so it's re-evaluated against
        // each document's LIVE state at the moment RavenDB actually applies that document's write -- not
        // against the (possibly stale) index snapshot used to build the candidate list above. RavenDB
        // serializes concurrent writes to the same document, so whichever of two racing
        // AcquireAndFetchAsync calls reaches a given document's write first sees it unlocked and claims
        // it; the other's script sees it already locked and its `if` guard skips that document entirely.
        // This is the RavenDB-native equivalent of SQL's outer WHERE re-check on the actual UPDATE
        // statement (not just its candidate CTE) -- same mechanism, same reason.
        var patchRql = $@"from '{CollectionName}' as e
where e.ClusterId = $clusterId and id() in ($candidateIds)
update {{
    if (this.PartitionLockId == null || this.PartitionLockExpiresAt < $lockNow) {{
        this.PartitionLockId = $partitionLockId;
        this.PartitionLockExpiresAt = $lockExpiresAt;
    }}
}}";
        // AllowStale=true -- safe for the same reason as BulkUpdateAsync's patch: the script's own if-guard
        // re-checks live PartitionLockId/PartitionLockExpiresAt, so a stale id() in (...) candidate set can
        // only under-claim, never double-claim.
        var patchOperation = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery
        {
            Query = patchRql,
            QueryParameters = new Parameters
            {
                ["clusterId"] = ClusterConnConfig.ClusterId,
                ["candidateIds"] = candidateIds,
                ["lockNow"] = lockNow,
                ["partitionLockId"] = partitionLockId,
                ["lockExpiresAt"] = lockExpiresAt,
            },
        }, new QueryOperationOptions { AllowStale = true }));
        await patchOperation.WaitForCompletionAsync();

        // Step 3: confirmatory read, by lock ownership (not by the candidate id list) -- this is what
        // actually determines what THIS caller won, independent of whatever the patch's rowsAffected count
        // says. This is the one query in the whole repository that keeps WaitForNonStaleResults -- see
        // AcquireConfirmationNonStaleTimeout's own comment for why. Matches SQL's QueryLockedJobsAsync,
        // which adds the identical "not already expired" guard using the same nowUtcWithSkew captured at
        // the top of the method (not a fresh "now" at confirm time) -- WaitForNonStaleResults can itself
        // take up to AcquireConfirmationNonStaleTimeout, so without this a very short-lived lease could
        // already read back as expired by the time this query returns; excluding it here matches SQL
        // rather than handing the caller a job whose lock the repository itself no longer considers held.
        using var confirmSession = store.OpenAsyncSession();
        var confirmRql = $"from '{CollectionName}' as e where e.ClusterId = $clusterId and e.PartitionLockId = $partitionLockId and e.PartitionLockExpiresAt > $lockNow";
        var confirmed = await confirmSession.Advanced.AsyncRawQuery<RavenDbJobDocument>(confirmRql)
            .WaitForNonStaleResults(AcquireConfirmationNonStaleTimeout)
            .AddParameter("clusterId", ClusterConnConfig.ClusterId)
            .AddParameter("partitionLockId", partitionLockId)
            .AddParameter("lockNow", lockNow)
            .ToListAsync();

        return confirmed.Select(doc => ToDomain(doc, confirmSession.Advanced.GetChangeVectorFor(doc))).ToList();
    }

    // ==================== BulkUpdateAsync (set-based) ====================

    public async Task BulkUpdateAsync(BulkJobUpdateRequest request)
    {
        if (request.JobIds.Count == 0 || request.Properties.Count == 0) return;

        var setStatements = new List<string>();
        var parameters = new Dictionary<string, object?>
        {
            ["clusterId"] = ClusterConnConfig.ClusterId,
            ["jobIds"] = request.JobIds.ToArray(),
        };

        var paramIndex = 0;
        foreach (var property in request.Properties)
        {
            foreach (var (rqlField, value) in BuildBulkUpdateAssignments(property))
            {
                var paramName = $"p{paramIndex++}";
                setStatements.Add($"this.{rqlField} = ${paramName};");
                parameters[paramName] = value;
            }
        }

        // ExcludeStatuses is re-checked INSIDE the update script against the live document (this.Status),
        // not just in the outer where clause -- the where clause only selects a (possibly stale-index)
        // candidate set by JobIds, but candidate selection being stale here would only risk a job briefly
        // being skipped, never wrongly matched. The script's own condition is what's authoritative, so
        // AllowStale=true on the query is safe: a stale candidate whose real current status is now
        // excluded still won't be touched, because the script checks live state, not the index snapshot.
        var scriptGuard = "true";
        if (request.ExcludeStatuses is { Count: > 0 })
        {
            parameters["excludeStatuses"] = request.ExcludeStatuses.Select(s => s.ToString()).ToArray();
            scriptGuard = "$excludeStatuses.indexOf(this.Status) === -1";
        }

        // No explicit "bump Version" step -- unlike SQL, Version here IS RavenDB's own change vector,
        // which every write (including a patch) advances automatically as a side effect.
        var rql = $@"from '{CollectionName}' as e
where e.ClusterId = $clusterId and e.JobId in ($jobIds)
update {{
    if ({scriptGuard}) {{
    {string.Join(" ", setStatements)}
    }}
}}";
        var queryParameters = new Parameters();
        foreach (var kv in parameters) queryParameters.Add(kv.Key, kv.Value);
        // AllowStale=true -- PatchByQueryOperation throws "Index is stale" by default instead of the
        // DeleteByQueryOperation-style silent-proceed. Safe here specifically because the script's own
        // guard re-checks live state (see comment above), so a stale candidate set can only under-select,
        // never mis-patch.
        var operation = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery
        {
            Query = rql,
            QueryParameters = queryParameters,
        }, new QueryOperationOptions { AllowStale = true }));
        await operation.WaitForCompletionAsync();
    }

    // Mirrors SQL's sql.ColumnNameFor(field.Expression) -- maps a JobRawModel property to the RavenDB
    // document field(s) it corresponds to, converting value-object properties (AgentConnectionId, HostId,
    // Timeout) to their flattened storage representation. HostId needs two fields set together since
    // HostId.IdValue alone doesn't carry HostDisplayName.
    private static List<(string RqlField, object? Value)> BuildBulkUpdateAssignments(BulkJobUpdateProperty property)
    {
        var propertyName = GetPropertyName(property.Expression);
        switch (propertyName)
        {
            case nameof(JobRawModel.Id):
                return new List<(string, object?)> { ("JobId", property.Value) };
            case nameof(JobRawModel.AgentConnectionId):
                var agentConnectionId = property.Value as AgentConnectionId;
                return new List<(string, object?)> { ("AgentConnectionId", agentConnectionId?.IdValue) };
            case nameof(JobRawModel.HostId):
                var hostId = property.Value as HostId;
                return new List<(string, object?)>
                {
                    ("HostId", hostId?.IdValue),
                    ("HostDisplayName", hostId?.HostDisplayName),
                };
            case nameof(JobRawModel.Timeout):
                var timeout = property.Value is TimeSpan ts ? ts : TimeSpan.Zero;
                return new List<(string, object?)> { ("TimeoutTicks", timeout.Ticks) };
            default:
                return new List<(string, object?)> { (propertyName, property.Value) };
        }
    }

    private static string GetPropertyName(Expression<Func<JobRawModel, object?>> expression)
    {
        var body = expression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            body = unary.Operand;
        }

        if (body is MemberExpression member)
        {
            return member.Member.Name;
        }

        throw new NotSupportedException($"Unsupported BulkJobUpdateProperty expression: {expression}");
    }

    // ==================== BulkUpdateAsync (per-row CAS) ====================

    // Single shared session for the common case (one bulk LoadAsync + one batch SaveChangesAsync -- 2
    // round trips regardless of how many rows), falling back to independent per-row sessions only when
    // the batch itself conflicts. This works because of two things, both verified against real RavenDB
    // rather than assumed: (1) SaveChangesAsync is genuinely all-or-nothing per session -- a batch of N
    // CAS stores where even one has a stale change vector rolls back the other N-1 too, confirmed via
    // RavenDbJobsRepositoryConformanceTests.SaveChangesAsync_WithMixedValidAndStaleChangeVectors_ShouldRollBackEntireBatch
    // (4 valid + 1 deliberately-stale write in one session, all 5 came back unchanged after the
    // exception); and (2) the in-memory version check below happens BEFORE anything is queued into that
    // shared session, so rows that are already known-stale at load time (the common case this overload
    // exists for -- e.g. BulkUpdate_Models_ShouldOnlyUpdate_RowsWithMatchingVersion's mix of matching and
    // deliberately-mismatched rows) never risk triggering a batch-wide rollback in the first place. Only a
    // row that goes stale DURING this call's own load-to-save window can still cause the batch to throw,
    // which is genuinely rare -- the per-row fallback exists for that case alone.
    public async Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobRawModels)
    {
        if (jobRawModels.Count == 0) return new List<JobRawModel>();

        using var session = store.OpenAsyncSession();
        var ids = jobRawModels.Select(j => DocumentId(j.Id)).ToArray();
        var existingDocs = await session.LoadAsync<RavenDbJobDocument>(ids);

        var pendingUpdates = new List<(JobRawModel Model, RavenDbJobDocument Document)>();
        foreach (var job in jobRawModels)
        {
            var existing = existingDocs[DocumentId(job.Id)];
            if (existing == null) continue; // Ghost job -- silently excluded, matches SQL.

            var existingChangeVector = session.Advanced.GetChangeVectorFor(existing);
            if (!string.Equals(job.Version ?? string.Empty, existingChangeVector ?? string.Empty, StringComparison.Ordinal))
            {
                continue; // Already known stale at load time -- silently excluded, never queued.
            }

            session.Advanced.Evict(existing);

            var doc = ToDocument(job);
            PreserveMetadata(doc, existing);

            await session.StoreAsync(doc, changeVector: job.Version ?? string.Empty, id: DocumentId(job.Id));
            ApplyJobCollectionMetadata(session.Advanced.GetMetadataFor(doc));
            pendingUpdates.Add((job, doc));
        }

        if (pendingUpdates.Count == 0) return new List<JobRawModel>();

        try
        {
            await session.SaveChangesAsync();
        }
        catch (ConcurrencyException ex)
        {
            // Some row in the batch was modified between our load and our save -- fall back to retrying
            // each pending row independently rather than losing the whole batch. Warn, not Error: this is
            // a handled, expected-to-be-rare race, not a failure -- the fallback recovers whatever it can.
            logger.Warn(
                $"BulkUpdateAsync batch of {pendingUpdates.Count} job(s) hit a change-vector conflict and fell back to per-row retry.",
                JobMasterLogCategory.Job,
                exception: ex);
            return await BulkUpdateFallbackPerRowAsync(pendingUpdates);
        }

        foreach (var (job, doc) in pendingUpdates)
        {
            job.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
        }

        return pendingUpdates.Select(x => x.Model).ToList();
    }

    private async Task<IList<JobRawModel>> BulkUpdateFallbackPerRowAsync(List<(JobRawModel Model, RavenDbJobDocument Document)> pendingUpdates)
    {
        var results = new System.Collections.Concurrent.ConcurrentBag<JobRawModel>();

        await JobMasterParallelUtil.ForEachAsync(
            pendingUpdates,
            new ParallelOptions { MaxDegreeOfParallelism = PerRowSessionMaxDegreeOfParallelism },
            async (pending, ct) =>
            {
                using var session = store.OpenAsyncSession();
                try
                {
                    await session.StoreAsync(pending.Document, changeVector: pending.Model.Version ?? string.Empty, id: DocumentId(pending.Model.Id), token: ct);
                    ApplyJobCollectionMetadata(session.Advanced.GetMetadataFor(pending.Document));
                    await session.SaveChangesAsync(ct);
                }
                catch (ConcurrencyException)
                {
                    return; // Lost the race during the batch attempt too -- silently excluded.
                }

                pending.Model.SetVersion(session.Advanced.GetChangeVectorFor(pending.Document) ?? string.Empty);
                results.Add(pending.Model);
            });

        return results.ToList();
    }

    // ==================== Purge / archive ====================

    public async Task<int> PurgeFinalizedAsync(DateTime cutoffUtc, int limit)
    {
        var jobIds = await QueryFinalizedIdsAsync(cutoffUtc, limit);
        if (jobIds.Count == 0) return 0;
        return await DeleteByIdsAsync(jobIds);
    }

    public async Task<IList<JobRawModel>> QueryFinalizedToPurgeAsync(DateTime cutoffUtc, int limit)
    {
        var cutoff = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Utc);
        using var session = store.OpenAsyncSession();
        var rql = $"from '{CollectionName}' as e where e.ClusterId = $clusterId and e.Status in ($finalStatuses) and e.FinalizedAt != null and e.FinalizedAt <= $cutoff order by e.FinalizedAt asc limit 0, $limit";
        var docs = await session.Advanced.AsyncRawQuery<RavenDbJobDocument>(rql)
            .AddParameter("clusterId", ClusterConnConfig.ClusterId)
            .AddParameter("finalStatuses", FinalStatuses)
            .AddParameter("cutoff", cutoff)
            .AddParameter("limit", limit)
            .ToListAsync();

        return docs.Select(doc => ToDomain(doc, session.Advanced.GetChangeVectorFor(doc))).ToList();
    }

    private async Task<IList<Guid>> QueryFinalizedIdsAsync(DateTime cutoffUtc, int limit)
    {
        var cutoff = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Utc);
        using var session = store.OpenAsyncSession();
        var rql = $"from '{CollectionName}' as e where e.ClusterId = $clusterId and e.Status in ($finalStatuses) and e.FinalizedAt != null and e.FinalizedAt <= $cutoff order by e.FinalizedAt asc select JobId limit 0, $limit";
        var results = await session.Advanced.AsyncRawQuery<JobIdProjection>(rql)
            .AddParameter("clusterId", ClusterConnConfig.ClusterId)
            .AddParameter("finalStatuses", FinalStatuses)
            .AddParameter("cutoff", cutoff)
            .AddParameter("limit", limit)
            .ToListAsync();

        return results.Select(r => r.JobId).ToList();
    }

    public async Task<int> DeleteByIdsAsync(IList<Guid> ids)
    {
        if (ids.Count == 0) return 0;

        var totalDeleted = 0;
        for (var i = 0; i < ids.Count; i += JobMasterConstants.MaxBatchSizeForBulkOperation)
        {
            var batch = ids.Skip(i).Take(JobMasterConstants.MaxBatchSizeForBulkOperation).ToArray();

            using var session = store.OpenAsyncSession();
            foreach (var id in batch)
            {
                session.Delete(DocumentId(id));
            }

            await session.SaveChangesAsync();
            totalDeleted += batch.Length;

            var execRql = $"from '{ExecutionCollectionName}' as e where e.ClusterId = $clusterId and e.JobId in ($jobIds)";
            var execOperation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery
            {
                Query = execRql,
                QueryParameters = new Parameters
                {
                    ["clusterId"] = ClusterConnConfig.ClusterId,
                    ["jobIds"] = batch,
                },
            }, new QueryOperationOptions { AllowStale = false }));
            await execOperation.WaitForCompletionAsync();
        }

        return totalDeleted;
    }
    
    public async Task BulkInsertIfNotExistsAsync(IList<JobRawModel> jobs)
    {
        if (jobs.Count == 0) return;

        using var session = store.OpenAsyncSession();
        var ids = jobs.Select(j => DocumentId(j.Id)).ToArray();
        var existingDocs = await session.LoadAsync<RavenDbJobDocument>(ids);

        var pendingInserts = new List<(JobRawModel Model, RavenDbJobDocument Document)>();
        foreach (var job in jobs)
        {
            if (existingDocs[DocumentId(job.Id)] != null) continue; // Already exists -- leave untouched.

            var doc = ToDocument(job);
            await session.StoreAsync(doc, changeVector: string.Empty, id: DocumentId(job.Id));
            ApplyJobCollectionMetadata(session.Advanced.GetMetadataFor(doc));
            pendingInserts.Add((job, doc));
        }

        if (pendingInserts.Count == 0) return;

        try
        {
            await session.SaveChangesAsync();
        }
        catch (ConcurrencyException ex)
        {
            // One of the "missing" jobs got inserted by someone else between our load and our save --
            // fall back to inserting each pending job independently rather than losing the whole batch.
            logger.Warn(
                $"BulkInsertIfNotExistsAsync batch of {pendingInserts.Count} job(s) hit a change-vector conflict and fell back to per-row retry.",
                JobMasterLogCategory.Job,
                exception: ex);
            await BulkInsertFallbackPerRowAsync(pendingInserts);
            return;
        }

        foreach (var (job, doc) in pendingInserts)
        {
            job.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
        }
    }

    private async Task BulkInsertFallbackPerRowAsync(List<(JobRawModel Model, RavenDbJobDocument Document)> pendingInserts)
    {
        await JobMasterParallelUtil.ForEachAsync(
            pendingInserts,
            new ParallelOptions { MaxDegreeOfParallelism = PerRowSessionMaxDegreeOfParallelism },
            async (pending, ct) =>
            {
                using var session = store.OpenAsyncSession();
                try
                {
                    await session.StoreAsync(pending.Document, changeVector: string.Empty, id: DocumentId(pending.Model.Id), token: ct);
                    ApplyJobCollectionMetadata(session.Advanced.GetMetadataFor(pending.Document));
                    await session.SaveChangesAsync(ct);
                }
                catch (ConcurrencyException)
                {
                    return; // Already exists now -- leave untouched, matches "insert if not exists" semantics.
                }

                pending.Model.SetVersion(session.Advanced.GetChangeVectorFor(pending.Document) ?? string.Empty);
            });
    }

    // ==================== WHERE builder ====================

    private (string Where, Dictionary<string, object?> Parameters) BuildWhereRql(JobQueryCriteria criteria, bool? isLocked)
    {
        var parameters = new Dictionary<string, object?> { ["clusterId"] = ClusterConnConfig.ClusterId };
        var where = new List<string> { "e.ClusterId = $clusterId" };

        if (criteria.Status.HasValue)
        {
            where.Add("e.Status = $status");
            parameters["status"] = criteria.Status.Value;
        }

        if (criteria.Statuses.Count > 0)
        {
            where.Add("e.Status in ($statuses)");
            parameters["statuses"] = criteria.Statuses.ToArray();
        }

        if (criteria.Priority.HasValue)
        {
            where.Add("e.Priority = $priority");
            parameters["priority"] = criteria.Priority.Value;
        }

        if (criteria.NextPlanExecutionAtFrom.HasValue)
        {
            where.Add("e.NextPlanExecutionAt >= $nextPlanFrom");
            parameters["nextPlanFrom"] = DateTime.SpecifyKind(criteria.NextPlanExecutionAtFrom.Value, DateTimeKind.Utc);
        }

        if (criteria.NextPlanExecutionAtTo.HasValue)
        {
            where.Add("e.NextPlanExecutionAt <= $nextPlanTo");
            parameters["nextPlanTo"] = DateTime.SpecifyKind(criteria.NextPlanExecutionAtTo.Value, DateTimeKind.Utc);
        }

        if (criteria.ScheduledFrom.HasValue)
        {
            where.Add("e.ScheduledAt >= $scheduledFrom");
            parameters["scheduledFrom"] = DateTime.SpecifyKind(criteria.ScheduledFrom.Value, DateTimeKind.Utc);
        }

        if (criteria.ScheduledTo.HasValue)
        {
            where.Add("e.ScheduledAt <= $scheduledTo");
            parameters["scheduledTo"] = DateTime.SpecifyKind(criteria.ScheduledTo.Value, DateTimeKind.Utc);
        }

        if (criteria.ProcessDeadlineTo.HasValue)
        {
            where.Add("e.ProcessDeadline != null and e.ProcessDeadline <= $processDeadlineTo");
            parameters["processDeadlineTo"] = DateTime.SpecifyKind(criteria.ProcessDeadlineTo.Value, DateTimeKind.Utc);
        }

        if (criteria.TriggerSourceTypes.Count > 0)
        {
            where.Add("e.TriggerSourceType in ($triggerSourceTypes)");
            parameters["triggerSourceTypes"] = criteria.TriggerSourceTypes.ToArray();
        }

        if (criteria.SourceId.HasValue)
        {
            where.Add("e.SourceId = $sourceId");
            parameters["sourceId"] = criteria.SourceId.Value;
        }

        if (criteria.SourceIds.Count > 0)
        {
            where.Add("e.SourceId in ($sourceIds)");
            parameters["sourceIds"] = criteria.SourceIds.ToArray();
        }

        if (!string.IsNullOrEmpty(criteria.JobDefinitionId))
        {
            where.Add("e.JobDefinitionId = $jobDefinitionId");
            parameters["jobDefinitionId"] = criteria.JobDefinitionId;
        }

        if (!string.IsNullOrEmpty(criteria.WorkerLane))
        {
            where.Add("e.WorkerLane = $workerLane");
            parameters["workerLane"] = criteria.WorkerLane;
        }

        if (!string.IsNullOrEmpty(criteria.BucketId))
        {
            where.Add("e.BucketId = $bucketId");
            parameters["bucketId"] = criteria.BucketId;
        }

        if (criteria.ExcludeBucketIds.Count > 0)
        {
            where.Add("not (e.BucketId in ($excludeBucketIds))");
            parameters["excludeBucketIds"] = criteria.ExcludeBucketIds.ToArray();
        }

        if (criteria.JobIds.Count > 0)
        {
            where.Add("e.JobId in ($jobIds)");
            parameters["jobIds"] = criteria.JobIds.ToArray();
        }

        if (criteria.ExcludeJobIds.Count > 0)
        {
            where.Add("not (e.JobId in ($excludeJobIds))");
            parameters["excludeJobIds"] = criteria.ExcludeJobIds.ToArray();
        }

        for (var i = 0; i < criteria.MetadataFilters.Count; i++)
        {
            var (clause, paramName, paramValue) = RavenDbFilterClauseBuilder.Build(criteria.MetadataFilters[i], i, "metaFilter", "Metadata");
            where.Add(clause);
            if (paramName != null) parameters[paramName] = paramValue;
        }

        if (isLocked == false)
        {
            where.Add("(e.PartitionLockId = null or e.PartitionLockExpiresAt < $lockNow)");
            parameters["lockNow"] = JobMasterConstants.NowUtcWithSkewTolerance();
        }

        return (string.Join(" and ", where), parameters);
    }

    // ==================== Mapping ====================

    private RavenDbJobDocument ToDocument(JobRawModel model)
    {
        var doc = new RavenDbJobDocument
        {
            JobId = model.Id,
            ClusterId = model.ClusterId,
            JobDefinitionId = model.JobDefinitionId,
            TriggerSourceType = model.TriggerSourceType,
            SourceId = model.SourceId,
            BucketId = model.BucketId,
            AgentConnectionId = model.AgentConnectionId?.IdValue,
            AgentWorkerId = model.AgentWorkerId,
            HostId = model.HostId?.IdValue,
            HostDisplayName = model.HostId?.HostDisplayName,
            Priority = model.Priority,
            ScheduledAt = DateTime.SpecifyKind(model.ScheduledAt, DateTimeKind.Utc),
            NextPlanExecutionAt = model.NextPlanExecutionAt.HasValue ? DateTime.SpecifyKind(model.NextPlanExecutionAt.Value, DateTimeKind.Utc) : null,
            MsgData = model.MsgData,
            Status = model.Status,
            NumberOfFailures = model.NumberOfFailures,
            TimeoutTicks = model.Timeout.Ticks,
            MaxNumberOfRetries = model.MaxNumberOfRetries,
            CreatedAt = DateTime.SpecifyKind(model.CreatedAt, DateTimeKind.Utc),
            PartitionLockId = model.PartitionLockId,
            PartitionLockExpiresAt = model.PartitionLockExpiresAt.HasValue ? DateTime.SpecifyKind(model.PartitionLockExpiresAt.Value, DateTimeKind.Utc) : null,
            ProcessDeadline = model.ProcessDeadline.HasValue ? DateTime.SpecifyKind(model.ProcessDeadline.Value, DateTimeKind.Utc) : null,
            ProcessStartedAt = model.ProcessStartedAt.HasValue ? DateTime.SpecifyKind(model.ProcessStartedAt.Value, DateTimeKind.Utc) : null,
            FinalizedAt = model.FinalizedAt.HasValue ? DateTime.SpecifyKind(model.FinalizedAt.Value, DateTimeKind.Utc) : null,
            WorkerLane = model.WorkerLane,
        };

        ApplyMetadata(doc, model.Metadata);
        return doc;
    }

    // MetadataJson is stored verbatim (returned as-is to callers -- zero round-trip risk regardless of
    // which of KeyValueBag's 11 scalar types it encodes). MetadataValues/DateTimeValues/GuidValues/
    // DecimalValues are a SEPARATE query-side-only projection for MetadataFilters, built fresh here every
    // write -- same 4-bucket dispatch as RavenDbGenericRecordDocument, sufficient because MetadataFilters
    // itself only ever targets string/int/datetime/guid/decimal, never the narrower short/byte/char types.
    private static void ApplyMetadata(RavenDbJobDocument doc, string? metadataJson)
    {
        doc.MetadataJson = metadataJson;
        doc.MetadataValues = new Dictionary<string, object?>();
        doc.MetadataDateTimeValues = new Dictionary<string, DateTime>();
        doc.MetadataGuidValues = new Dictionary<string, Guid>();
        doc.MetadataDecimalValues = new Dictionary<string, decimal>();

        if (string.IsNullOrEmpty(metadataJson)) return;

        var dict = KeyValueBagUtil.DeserializeMetadata(metadataJson).ToDictionary();
        foreach (var kv in dict)
        {
            switch (kv.Value)
            {
                case DateTime dt:
                    doc.MetadataDateTimeValues[kv.Key] = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                    break;
                case Guid g:
                    doc.MetadataGuidValues[kv.Key] = g;
                    break;
                case decimal dec:
                    doc.MetadataDecimalValues[kv.Key] = dec;
                    break;
                default:
                    doc.MetadataValues[kv.Key] = kv.Value;
                    break;
            }
        }
    }

    private static JobRawModel ToDomain(RavenDbJobDocument doc, string? version)
    {
        var model = new JobRawModel(doc.ClusterId)
        {
            Id = doc.JobId,
            JobDefinitionId = doc.JobDefinitionId,
            TriggerSourceType = doc.TriggerSourceType,
            SourceId = doc.SourceId,
            BucketId = doc.BucketId,
            AgentConnectionId = !string.IsNullOrEmpty(doc.AgentConnectionId) ? new AgentConnectionId(doc.AgentConnectionId!) : null,
            AgentWorkerId = doc.AgentWorkerId,
            Priority = doc.Priority,
            ScheduledAt = doc.ScheduledAt,
            NextPlanExecutionAt = doc.NextPlanExecutionAt,
            MsgData = string.IsNullOrEmpty(doc.MsgData) ? "{}" : doc.MsgData,
            Metadata = doc.MetadataJson,
            Status = doc.Status,
            NumberOfFailures = doc.NumberOfFailures,
            Timeout = TimeSpan.FromTicks(doc.TimeoutTicks),
            MaxNumberOfRetries = doc.MaxNumberOfRetries,
            CreatedAt = doc.CreatedAt,
            PartitionLockId = doc.PartitionLockId,
            PartitionLockExpiresAt = doc.PartitionLockExpiresAt,
            ProcessDeadline = doc.ProcessDeadline,
            ProcessStartedAt = doc.ProcessStartedAt,
            FinalizedAt = doc.FinalizedAt,
            WorkerLane = doc.WorkerLane,
            HostId = !string.IsNullOrEmpty(doc.HostId) && !string.IsNullOrEmpty(doc.HostDisplayName)
                ? HostId.Recover(doc.HostDisplayName!, doc.HostId!)
                : null,
        };

        model.SetVersion(version ?? string.Empty);
        return model;
    }

    private static JobExecution ToDomain(RavenDbJobExecutionDocument doc)
    {
        var execution = new JobExecution(doc.ClusterId);
        execution.Id = doc.ExecutionId;
        execution.JobId = doc.JobId;
        execution.StartedAt = doc.StartedAt;
        execution.AgentWorkerId = doc.AgentWorkerId;
        execution.BucketId = doc.BucketId;
        execution.FinalizedAt = doc.FinalizedAt;
        execution.OutcomeMessage = doc.OutcomeMessage;
        execution.Outcome = doc.Outcome;

        if (!string.IsNullOrEmpty(doc.AgentConnectionId))
        {
            execution.AgentConnectionId = new AgentConnectionId(doc.AgentConnectionId!);
        }

        if (!string.IsNullOrEmpty(doc.HostId))
        {
            execution.HostId = HostId.Recover(doc.HostDisplayName ?? string.Empty, doc.HostId!);
        }

        return execution;
    }

    private RavenDbJobExecutionDocument ToExecutionDocument(JobExecution execution) => new()
    {
        ExecutionId = execution.Id,
        ClusterId = execution.ClusterId,
        JobId = execution.JobId,
        StartedAt = DateTime.SpecifyKind(execution.StartedAt, DateTimeKind.Utc),
        AgentConnectionId = execution.AgentConnectionId?.IdValue,
        AgentWorkerId = execution.AgentWorkerId,
        BucketId = execution.BucketId,
        HostId = execution.HostId?.IdValue,
        HostDisplayName = execution.HostId?.HostDisplayName,
        FinalizedAt = execution.FinalizedAt.HasValue ? DateTime.SpecifyKind(execution.FinalizedAt.Value, DateTimeKind.Utc) : null,
        OutcomeMessage = execution.OutcomeMessage,
        Outcome = execution.Outcome,
    };

    private void ApplyExecutionCollectionMetadata(Raven.Client.Documents.Session.IMetadataDictionary metadata)
    {
        metadata[Constants.Documents.Metadata.Collection] = ExecutionCollectionName;
    }

    private void ApplyJobCollectionMetadata(Raven.Client.Documents.Session.IMetadataDictionary metadata)
    {
        metadata[Constants.Documents.Metadata.Collection] = CollectionName;
    }
}
