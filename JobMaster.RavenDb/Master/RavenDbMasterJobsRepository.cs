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
using JobMaster.Sdk.Utils.Extensions;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;

namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbMasterJobsRepository : JobMasterClusterAwareRepository, IMasterJobsRepository
{
    private const int PerRowSessionMaxDegreeOfParallelism = 10;

    // Tier-2 fallback granularity -- see SaveWithPartitionedFallbackAsync.
    private const int FallbackPartitionSize = 5;

    private static readonly TimeSpan ExecutionCleanupNonStaleTimeout = TimeSpan.FromSeconds(10);
    
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
        store = storeManager.GetOrCreateStore(clusterConnectionConfig.ConnectionString, clusterConnectionConfig.GetCertificate());
        this.logger = logger;
    }

    public override string MasterRepoTypeId => RavenDbRepositoryConstants.RepositoryTypeId;

    private string CollectionName => RavenDbDocumentNaming.CollectionName(ClusterConnConfig, RavenDbCollectionNames.Job);
    private string ExecutionCollectionName => RavenDbDocumentNaming.CollectionName(ClusterConnConfig, RavenDbCollectionNames.JobExecution);
    private string DocumentId(Guid jobId) => RavenDbDocumentNaming.DocumentId(ClusterConnConfig, RavenDbCollectionNames.Job, jobId.ToString("N"));
    private string ExecutionDocumentId(Guid executionId) => RavenDbDocumentNaming.DocumentId(ClusterConnConfig, RavenDbCollectionNames.JobExecution, executionId.ToString("N"));

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

        // Not a group-by aggregate -- RavenDB's group-by doesn't support min(), and WHERE can't precede it.
        // RavenDB also sorts nulls FIRST ascending (unlike SQL's MIN(), which ignores them), so a second
        // query (excluding nulls) only runs when row 0's NextPlanExecutionAt is null.
        using var session = store.OpenAsyncSession();
        var (where, parameters) = BuildWhereRql(queryCriteria, isLocked: false);

        var indexName = TryGetApplicableIndexName(queryCriteria, RavenDbJobIndexes.NextPlanExecutionAtField);
        var from = indexName != null ? $"index '{indexName}'" : $"'{CollectionName}'";

        var rql = $"from {from} as e where {where} order by e.NextPlanExecutionAt asc limit 0, 1";
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
            var minRql = $"from {from} as e where {where} and e.NextPlanExecutionAt != null order by e.NextPlanExecutionAt asc limit 0, 1";
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

        // Step 1: Select candidates to acquire
        using var candidateSession = store.OpenAsyncSession();
        var (where, parameters) = BuildWhereRql(queryCriteria, isLocked: false);
        var orderByField = queryCriteria.SortBy != null ? MapSortProperty(queryCriteria.SortBy.Property) : RavenDbJobIndexes.NextPlanExecutionAtField;
        var orderBy = queryCriteria.SortBy != null
            ? $"order by e.{orderByField} {(queryCriteria.SortBy.Ascending ? "asc" : "desc")}"
            : $"order by e.{orderByField} asc";
        parameters["offset"] = queryCriteria.Offset;
        parameters["limit"] = queryCriteria.CountLimit;
        var indexName = TryGetApplicableIndexName(queryCriteria, orderByField);
        var candidateFrom = indexName != null ? $"index '{indexName}'" : $"'{CollectionName}'";
        var candidateRql = $"from {candidateFrom} as e where {where} {orderBy} select id() as Id limit $offset, $limit";
        var candidateQuery = candidateSession.Advanced.AsyncRawQuery<IdProjection>(candidateRql);
        foreach (var kv in parameters) candidateQuery = candidateQuery.AddParameter(kv.Key, kv.Value);
        var candidateIds = (await candidateQuery.ToListAsync()).Select(p => p.Id).ToArray();

        if (candidateIds.Length == 0) return new List<JobRawModel>();

        // Step 2: Patch unlock documents.
        var patchRql = $@"from '{CollectionName}' as e
where e.ClusterId = $clusterId and id() in ($candidateIds)
update {{
    if (this.PartitionLockId == null || this.PartitionLockExpiresAt < $lockNow) {{
        this.PartitionLockId = $partitionLockId;
        this.PartitionLockExpiresAt = $lockExpiresAt;
    }}
}}";
        // AllowStale=true -- the script's own if-guard re-checks live state, so a stale candidate set can
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

        // Step 3: confirm by getting the documents and filtering locked based on partitionLockId passed in
        using var confirmSession = store.OpenAsyncSession();
        var candidateDocs = await confirmSession.LoadAsync<RavenDbJobDocument>(candidateIds);
        var confirmed = candidateDocs.Values
            .Where(doc => doc != null && doc.PartitionLockId == partitionLockId && doc.PartitionLockExpiresAt > lockNow)
            .ToList();

        return confirmed.Select(doc => ToDomain(doc!, confirmSession.Advanced.GetChangeVectorFor(doc!))).ToList();
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

        // ExcludeStatuses is re-checked INSIDE the update script against live this.Status, not just the
        // outer where -- so a stale candidate set can only under-select, never mis-patch.
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
        // AllowStale=true -- safe because the script's own guard above re-checks live state.
        var operation = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery
        {
            Query = rql,
            QueryParameters = queryParameters,
        }, new QueryOperationOptions { AllowStale = true }));
        await operation.WaitForCompletionAsync();
    }

    // Maps a JobRawModel property to its RavenDB field(s) -- HostId needs two fields set together since
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

    // Single shared session for the common case (bulk Load + one SaveChangesAsync), falling back to
    // per-row sessions only when the batch itself conflicts -- SaveChangesAsync is all-or-nothing per
    // session, so a batch failure means nothing from it was written yet.
    public async Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobRawModels)
    {
        if (jobRawModels.Count == 0) return new List<JobRawModel>();

        try
        {
            return await InternalBulkUpdateAsync(jobRawModels);
        }
        catch (ConcurrencyException ex)
        {
            logger.Warn(
                $"BulkUpdateAsync batch of {jobRawModels.Count} job(s) hit a change-vector conflict and fell back to partitions of {FallbackPartitionSize}.",
                JobMasterLogCategory.Job,
                exception: ex);

            var result = new List<JobRawModel>();
            var (succeededJobs, failedJobs) = await BulkUpdateFallbackPerPartitionAsync(jobRawModels);
            result.AddRange(succeededJobs);

            if (failedJobs.Count > 0)
            {
                result.AddRange(await BulkUpdateFallbackPerRowAsync(failedJobs));
            }

            return result;
        }
    }

    private async Task<(IList<JobRawModel> SucceededJobs, IList<JobRawModel> FailedJobs)> BulkUpdateFallbackPerPartitionAsync(IList<JobRawModel> jobRawModels)
    {
        var failedJobs = new List<JobRawModel>();
        var succeededJobs = new List<JobRawModel>();

        foreach (var partition in jobRawModels.Partition(FallbackPartitionSize))
        {
            try
            {
                // Must use InternalBulkUpdateAsync's return value, not the input partition -- it silently
                // excludes ghost jobs and already-stale versions.
                var saved = await InternalBulkUpdateAsync(partition.ToList());
                succeededJobs.AddRange(saved);
            }
            catch (Exception ex)
            {
                logger.Error(
                    $"BulkUpdateAsync partition of {partition.Count} job(s) failed and fell back to per-row retry.",
                    JobMasterLogCategory.Job,
                    exception: ex);
                failedJobs.AddRange(partition);
            }
        }

        return (succeededJobs, failedJobs);
    }

    // Pure "save this list" unit, no exception handling -- reusable at any granularity.
    private async Task<IList<JobRawModel>> InternalBulkUpdateAsync(IList<JobRawModel> jobRawModels)
    {
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

        await session.SaveChangesAsync();

        foreach (var (job, doc) in pendingUpdates)
        {
            job.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
        }

        return pendingUpdates.Select(x => x.Model).ToList();
    }

    private async Task<IList<JobRawModel>> BulkUpdateFallbackPerRowAsync(IList<JobRawModel> jobRawModels)
    {
        var results = new System.Collections.Concurrent.ConcurrentBag<JobRawModel>();

        await JobMasterParallelUtil.ForEachAsync(
            jobRawModels,
            new ParallelOptions { MaxDegreeOfParallelism = PerRowSessionMaxDegreeOfParallelism },
            async (job, _) =>
            {
                IList<JobRawModel> saved;
                try
                {
                    saved = await InternalBulkUpdateAsync(new List<JobRawModel> { job });
                }
                catch (ConcurrencyException)
                {
                    return; // Lost the race during the batch attempt too -- silently excluded.
                }

                foreach (var s in saved) results.Add(s);
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

            // AllowStale = false fails immediately ("Index is stale") instead of waiting -- StaleTimeout
            // makes it wait, since job-execution rows written just before a purge can still be indexing.
            var execRql = $"from '{ExecutionCollectionName}' as e where e.ClusterId = $clusterId and e.JobId in ($jobIds)";
            var execOperation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery
            {
                Query = execRql,
                QueryParameters = new Parameters
                {
                    ["clusterId"] = ClusterConnConfig.ClusterId,
                    ["jobIds"] = batch,
                },
            }, new QueryOperationOptions { AllowStale = false, StaleTimeout = ExecutionCleanupNonStaleTimeout }));
            await execOperation.WaitForCompletionAsync();
        }

        return totalDeleted;
    }
    
    public async Task BulkInsertIfNotExistsAsync(IList<JobRawModel> jobs)
    {
        if (jobs.Count == 0) return;

        try
        {
            await InternalBulkInsertIfNotExistsAsync(jobs);
        }
        catch (ConcurrencyException ex)
        {
            // One of the "missing" jobs got inserted by someone else between our load and our save.
            logger.Warn(
                $"BulkInsertIfNotExistsAsync batch of {jobs.Count} job(s) hit a change-vector conflict and fell back to partitions of {FallbackPartitionSize}.",
                JobMasterLogCategory.Job,
                exception: ex);

            var (_, failedJobs) = await BulkInsertFallbackPerPartitionAsync(jobs);
            if (failedJobs.Count > 0)
            {
                await BulkInsertFallbackPerRowAsync(failedJobs);
            }
        }
    }

    // Mirrors InternalBulkUpdateAsync -- "insert if not present" unit, reused at every fallback granularity.
    private async Task<IList<JobRawModel>> InternalBulkInsertIfNotExistsAsync(IList<JobRawModel> jobs)
    {
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

        if (pendingInserts.Count == 0) return new List<JobRawModel>();

        await session.SaveChangesAsync();

        foreach (var (job, doc) in pendingInserts)
        {
            job.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
        }

        return pendingInserts.Select(x => x.Model).ToList();
    }

    private async Task<(IList<JobRawModel> SucceededJobs, IList<JobRawModel> FailedJobs)> BulkInsertFallbackPerPartitionAsync(IList<JobRawModel> jobs)
    {
        var failedJobs = new List<JobRawModel>();
        var succeededJobs = new List<JobRawModel>();

        foreach (var partition in jobs.Partition(FallbackPartitionSize))
        {
            try
            {
                var saved = await InternalBulkInsertIfNotExistsAsync(partition.ToList());
                succeededJobs.AddRange(saved);
            }
            catch (Exception ex)
            {
                logger.Error(
                    $"BulkInsertIfNotExistsAsync partition of {partition.Count} job(s) failed and fell back to per-row retry.",
                    JobMasterLogCategory.Job,
                    exception: ex);
                failedJobs.AddRange(partition);
            }
        }

        return (succeededJobs, failedJobs);
    }

    private async Task BulkInsertFallbackPerRowAsync(IList<JobRawModel> jobs)
    {
        await JobMasterParallelUtil.ForEachAsync(
            jobs,
            new ParallelOptions { MaxDegreeOfParallelism = PerRowSessionMaxDegreeOfParallelism },
            async (job, _) =>
            {
                try
                {
                    await InternalBulkInsertIfNotExistsAsync(new List<JobRawModel> { job });
                }
                catch (ConcurrencyException)
                {
                    // Already exists now -- leave untouched, matches "insert if not exists" semantics.
                }
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

    // Returns the deployed static index this call's criteria can safely use, or null to fall back to a
    // dynamic query -- errs conservative since a field the index doesn't map throws in RavenDB.
    private static string? TryGetApplicableIndexName(JobQueryCriteria criteria, string orderByField)
    {
        if (criteria.Statuses.Count > 0) return null;
        if (criteria.Priority.HasValue) return null;
        if (criteria.ScheduledFrom.HasValue || criteria.ScheduledTo.HasValue) return null;
        if (criteria.ProcessDeadlineTo.HasValue) return null;
        if (criteria.TriggerSourceTypes.Count > 0) return null;
        if (criteria.SourceId.HasValue || criteria.SourceIds.Count > 0) return null;
        if (!string.IsNullOrEmpty(criteria.JobDefinitionId)) return null;
        if (!string.IsNullOrEmpty(criteria.WorkerLane)) return null;
        if (!string.IsNullOrEmpty(criteria.BucketId)) return null;
        if (criteria.ExcludeBucketIds.Count > 0) return null;
        if (criteria.JobIds.Count > 0) return null;
        if (criteria.ExcludeJobIds.Count > 0) return null;
        if (criteria.MetadataFilters.Count > 0) return null;
        if (!string.Equals(orderByField, RavenDbJobIndexes.NextPlanExecutionAtField, StringComparison.Ordinal)) return null;

        // PartitionLockId/PartitionLockExpiresAt are always included -- every caller (AcquireAndFetchAsync,
        // ProbeForAcquireAsync) only ever queries isLocked: false candidates.
        var tokens = new List<string> { RavenDbJobIndexes.ClusterIdField };
        if (criteria.Status.HasValue) tokens.Add(RavenDbJobIndexes.StatusField);
        tokens.Add(RavenDbJobIndexes.PartitionLockIdField);
        tokens.Add(RavenDbJobIndexes.PartitionLockExpiresAtField);
        tokens.Add(RavenDbJobIndexes.NextPlanExecutionAtField);

        var requiredPrefix = "RavenDbJobs/" + string.Join("_", tokens);
        return RavenDbJobIndexes.DeployedIndexNames.FirstOrDefault(name => name.StartsWith(requiredPrefix, StringComparison.Ordinal));
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

    // MetadataJson is stored verbatim. MetadataValues/DateTimeValues/GuidValues/DecimalValues are a
    // separate query-side-only projection for MetadataFilters, rebuilt fresh every write -- same 4-bucket
    // dispatch as RavenDbGenericRecordDocument.
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
