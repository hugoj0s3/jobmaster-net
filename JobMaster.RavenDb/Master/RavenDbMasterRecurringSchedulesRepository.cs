using JobMaster.Abstractions.Models;
using JobMaster.RavenDb.Connections;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
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

internal sealed class RavenDbMasterRecurringSchedulesRepository : JobMasterClusterAwareRepository, IMasterRecurringSchedulesRepository
{
    private const string Collection = "RecurringSchedule";

    private const int PerRowSessionMaxDegreeOfParallelism = 10;

    // Tier-2 fallback granularity -- see BulkInsertFallbackPerPartitionAsync.
    private const int FallbackPartitionSize = 5;

    // Used by GetByStaticId -- the one read here that needs to see a very recent write.
    private static readonly TimeSpan NonStaleResultsTimeout = TimeSpan.FromSeconds(2);

    private static readonly RecurringScheduleStatus[] TerminatedStatuses =
    {
        RecurringScheduleStatus.Inactive, RecurringScheduleStatus.Canceled, RecurringScheduleStatus.Completed,
    };

    private readonly IDocumentStore store;
    private readonly IJobMasterLogger logger;

    public RavenDbMasterRecurringSchedulesRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IRavenDbDocumentStoreManager storeManager,
        IJobMasterLogger logger) : base(clusterConnectionConfig)
    {
        store = storeManager.GetOrCreateStore(clusterConnectionConfig.ConnectionString, clusterConnectionConfig.GetCertificate());
        this.logger = logger;
    }

    public override string MasterRepoTypeId => RavenDbRepositoryConstants.RepositoryTypeId;

    private string CollectionName => RavenDbDocumentNaming.CollectionName(ClusterConnConfig, Collection);
    private string DocumentId(Guid scheduleId) => RavenDbDocumentNaming.DocumentId(ClusterConnConfig, Collection, scheduleId.ToString("N"));

    // ==================== Add ====================

    public void Add(RecurringScheduleRawModel scheduleRaw)
    {
        using var session = store.OpenSession();
        var doc = ToDocument(scheduleRaw);
        try
        {
            session.Store(doc, changeVector: string.Empty, id: DocumentId(scheduleRaw.Id));
            ApplyCollectionMetadata(session.Advanced.GetMetadataFor(doc));
            session.SaveChanges();
        }
        catch (ConcurrencyException ex)
        {
            throw new JobMasterDuplicationException(scheduleRaw.Id, "RecurringSchedule", ex);
        }

        scheduleRaw.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
    }

    public async Task AddAsync(RecurringScheduleRawModel scheduleRaw)
    {
        using var session = store.OpenAsyncSession();
        var doc = ToDocument(scheduleRaw);
        try
        {
            await session.StoreAsync(doc, changeVector: string.Empty, id: DocumentId(scheduleRaw.Id));
            ApplyCollectionMetadata(session.Advanced.GetMetadataFor(doc));
            await session.SaveChangesAsync();
        }
        catch (ConcurrencyException ex)
        {
            throw new JobMasterDuplicationException(scheduleRaw.Id, "RecurringSchedule", ex);
        }

        scheduleRaw.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
    }

    // ==================== Update ====================

    public void Update(RecurringScheduleRawModel scheduleRaw)
    {
        using var session = store.OpenSession();
        var existing = session.Load<RavenDbRecurringScheduleDocument>(DocumentId(scheduleRaw.Id));
        if (existing == null) return; // Matches SQL: updating a schedule that doesn't exist at all is a silent no-op.

        session.Advanced.Evict(existing);

        var doc = ToDocument(scheduleRaw);
        PreserveMetadata(doc, existing);

        try
        {
            session.Store(doc, changeVector: scheduleRaw.Version ?? string.Empty, id: DocumentId(scheduleRaw.Id));
            ApplyCollectionMetadata(session.Advanced.GetMetadataFor(doc));
            session.SaveChanges();
        }
        catch (ConcurrencyException)
        {
            throw new JobMasterVersionConflictException(scheduleRaw.Id, "RecurringSchedule", scheduleRaw.Version);
        }

        scheduleRaw.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
    }

    public async Task UpdateAsync(RecurringScheduleRawModel scheduleRaw)
    {
        using var session = store.OpenAsyncSession();
        var existing = await session.LoadAsync<RavenDbRecurringScheduleDocument>(DocumentId(scheduleRaw.Id));
        if (existing == null) return;

        session.Advanced.Evict(existing);

        var doc = ToDocument(scheduleRaw);
        PreserveMetadata(doc, existing);

        try
        {
            await session.StoreAsync(doc, changeVector: scheduleRaw.Version ?? string.Empty, id: DocumentId(scheduleRaw.Id));
            ApplyCollectionMetadata(session.Advanced.GetMetadataFor(doc));
            await session.SaveChangesAsync();
        }
        catch (ConcurrencyException)
        {
            throw new JobMasterVersionConflictException(scheduleRaw.Id, "RecurringSchedule", scheduleRaw.Version);
        }

        scheduleRaw.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
    }

    // Metadata is immutable after insert (conformance-verified, matches Jobs) -- Update must never let the
    // incoming model's Metadata overwrite what's already persisted.
    private static void PreserveMetadata(RavenDbRecurringScheduleDocument target, RavenDbRecurringScheduleDocument existing)
    {
        target.MetadataJson = existing.MetadataJson;
        target.MetadataValues = existing.MetadataValues;
        target.MetadataDateTimeValues = existing.MetadataDateTimeValues;
        target.MetadataGuidValues = existing.MetadataGuidValues;
        target.MetadataDecimalValues = existing.MetadataDecimalValues;
    }

    // ==================== Exists / Get ====================

    public bool Exists(Guid recurringScheduleId)
    {
        using var session = store.OpenSession();
        return session.Advanced.Exists(DocumentId(recurringScheduleId));
    }

    public async Task<bool> ExistsAsync(Guid recurringScheduleId)
    {
        using var session = store.OpenAsyncSession();
        return await session.Advanced.ExistsAsync(DocumentId(recurringScheduleId));
    }

    public RecurringScheduleRawModel? Get(Guid recurringScheduleId)
    {
        using var session = store.OpenSession();
        var doc = session.Load<RavenDbRecurringScheduleDocument>(DocumentId(recurringScheduleId));
        return doc == null ? null : ToDomain(doc, session.Advanced.GetChangeVectorFor(doc));
    }

    public async Task<RecurringScheduleRawModel?> GetAsync(Guid recurringScheduleId)
    {
        using var session = store.OpenAsyncSession();
        var doc = await session.LoadAsync<RavenDbRecurringScheduleDocument>(DocumentId(recurringScheduleId));
        return doc == null ? null : ToDomain(doc, session.Advanced.GetChangeVectorFor(doc));
    }

    public RecurringScheduleRawModel? GetByStaticId(string staticId)
    {
        using var session = store.OpenSession();
        var rql = $"from '{CollectionName}' as e where e.ClusterId = $clusterId and e.StaticDefinitionId = $staticId and e.RecurringScheduleType = $staticType limit 0, 1";
        var doc = session.Advanced.RawQuery<RavenDbRecurringScheduleDocument>(rql)
            .WaitForNonStaleResults(NonStaleResultsTimeout)
            .AddParameter("clusterId", ClusterConnConfig.ClusterId)
            .AddParameter("staticId", staticId)
            .AddParameter("staticType", RecurringScheduleType.Static)
            .ToList()
            .FirstOrDefault();

        return doc == null ? null : ToDomain(doc, session.Advanced.GetChangeVectorFor(doc));
    }

    // ==================== Query / Count ====================

    public IList<RecurringScheduleRawModel> Query(RecurringScheduleQueryCriteria queryCriteria)
    {
        using var session = store.OpenSession();
        var (rql, parameters) = BuildQueryRql(queryCriteria);
        var query = session.Advanced.RawQuery<RavenDbRecurringScheduleDocument>(rql);
        foreach (var kv in parameters) query = query.AddParameter(kv.Key, kv.Value);

        var docs = query.ToList();
        return docs.Select(doc => ToDomain(doc, session.Advanced.GetChangeVectorFor(doc))).ToList();
    }

    public async Task<IList<RecurringScheduleRawModel>> QueryAsync(RecurringScheduleQueryCriteria queryCriteria)
    {
        using var session = store.OpenAsyncSession();
        var (rql, parameters) = BuildQueryRql(queryCriteria);
        var query = session.Advanced.AsyncRawQuery<RavenDbRecurringScheduleDocument>(rql);
        foreach (var kv in parameters) query = query.AddParameter(kv.Key, kv.Value);

        var docs = await query.ToListAsync();
        return docs.Select(doc => ToDomain(doc, session.Advanced.GetChangeVectorFor(doc))).ToList();
    }

    public long Count(RecurringScheduleQueryCriteria queryCriteria)
    {
        using var session = store.OpenSession();
        var (where, parameters) = BuildWhereRql(queryCriteria, isLocked: null);
        var rql = $"from '{CollectionName}' as e where {where} limit 0, 1";
        var query = session.Advanced.RawQuery<RavenDbRecurringScheduleDocument>(rql).Statistics(out var stats);
        foreach (var kv in parameters) query = query.AddParameter(kv.Key, kv.Value);
        query.ToList();
        return stats.TotalResults;
    }

    private (string Rql, Dictionary<string, object?> Parameters) BuildQueryRql(RecurringScheduleQueryCriteria criteria)
    {
        var (where, parameters) = BuildWhereRql(criteria, isLocked: null);
        var orderBy = criteria.SortBy != null
            ? $"order by e.{MapSortProperty(criteria.SortBy.Property)} {(criteria.SortBy.Ascending ? "asc" : "desc")}"
            : "order by e.LastPlanCoverageUntil desc, e.CreatedAt asc";
        parameters["offset"] = criteria.Offset;
        parameters["limit"] = criteria.CountLimit;
        var rql = $"from '{CollectionName}' as e where {where} {orderBy} limit $offset, $limit";
        return (rql, parameters);
    }

    private static string MapSortProperty(string property) => property == nameof(RecurringScheduleRawModel.Id) ? "ScheduleId" : property;

    // ==================== ProbeCountForAcquireAsync ====================

    public async Task<long> ProbeCountForAcquireAsync(RecurringScheduleQueryCriteria queryCriteria)
    {
        if (queryCriteria.MetadataFilters.Count > 0)
        {
            throw new NotSupportedException("MetadataFilters are not supported by ProbeCountForAcquireAsync.");
        }

        // Simpler than Jobs' ProbeForAcquireAsync -- only a count is needed, so Statistics().TotalResults
        // on a payload-free query is enough.
        using var session = store.OpenAsyncSession();
        var (where, parameters) = BuildWhereRql(queryCriteria, isLocked: false);
        var rql = $"from '{CollectionName}' as e where {where} limit 0, 0";
        var query = session.Advanced.AsyncRawQuery<RavenDbRecurringScheduleDocument>(rql).Statistics(out var stats);
        foreach (var kv in parameters) query = query.AddParameter(kv.Key, kv.Value);
        await query.ToListAsync();
        return stats.TotalResults;
    }

    private sealed class IdProjection
    {
        public string Id { get; set; } = string.Empty;
    }

    private sealed class ScheduleIdProjection
    {
        public Guid ScheduleId { get; set; }
    }

    // ==================== AcquireAndFetchAsync ====================

    public async Task<IList<RecurringScheduleRawModel>> AcquireAndFetchAsync(RecurringScheduleQueryCriteria queryCriteria, Guid partitionLockId, DateTime expiresAtUtc)
    {
        if (partitionLockId == Guid.Empty) throw new ArgumentException("partitionLockId must not be empty", nameof(partitionLockId));

        var lockNow = JobMasterConstants.NowUtcWithSkewTolerance();
        var lockExpiresAt = DateTime.SpecifyKind(expiresAtUtc, DateTimeKind.Utc);

        // Step 1: candidate selection -- see RavenDbMasterJobsRepository.AcquireAndFetchAsync for the
        // full reasoning (identical shape here).
        using var candidateSession = store.OpenAsyncSession();
        var (where, parameters) = BuildWhereRql(queryCriteria, isLocked: false);
        var orderBy = queryCriteria.SortBy != null
            ? $"order by e.{MapSortProperty(queryCriteria.SortBy.Property)} {(queryCriteria.SortBy.Ascending ? "asc" : "desc")}"
            : "order by e.LastPlanCoverageUntil desc, e.CreatedAt asc";
        parameters["offset"] = queryCriteria.Offset;
        parameters["limit"] = queryCriteria.CountLimit;
        var candidateRql = $"from '{CollectionName}' as e where {where} {orderBy} select id() as Id limit $offset, $limit";
        var candidateQuery = candidateSession.Advanced.AsyncRawQuery<IdProjection>(candidateRql);
        foreach (var kv in parameters) candidateQuery = candidateQuery.AddParameter(kv.Key, kv.Value);
        var candidateIds = (await candidateQuery.ToListAsync()).Select(p => p.Id).ToArray();

        if (candidateIds.Length == 0) return new List<RecurringScheduleRawModel>();

        // Step 2: server-side patch, one round trip covering every candidate, live-state re-check inside
        // the update script -- same mechanism as Jobs.
        var patchRql = $@"from '{CollectionName}' as e
where e.ClusterId = $clusterId and id() in ($candidateIds)
update {{
    if (this.PartitionLockId == null || this.PartitionLockExpiresAt < $lockNow) {{
        this.PartitionLockId = $partitionLockId;
        this.PartitionLockExpiresAt = $lockExpiresAt;
    }}
}}";
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

        // Step 3: confirm via bulk point lookup (Load) -- see RavenDbMasterJobsRepository.AcquireAndFetchAsync.
        using var confirmSession = store.OpenAsyncSession();
        var candidateDocs = await confirmSession.LoadAsync<RavenDbRecurringScheduleDocument>(candidateIds);
        var confirmed = candidateDocs.Values
            .Where(doc => doc != null && doc.PartitionLockId == partitionLockId && doc.PartitionLockExpiresAt > lockNow)
            .ToList();

        return confirmed.Select(doc => ToDomain(doc!, confirmSession.Advanced.GetChangeVectorFor(doc!))).ToList();
    }

    // ==================== Static-definition bulk helpers ====================

    // Live-state re-check inside the update script -- a stale outer WHERE can only under-select, never
    // wrongly touch a row already re-ensured more recently.
    public void BulkUpdateStaticDefinitionLastEnsuredByStaticIds(IList<string> staticDefinitionIds, DateTime ensuredAt)
    {
        if (staticDefinitionIds.Count == 0) return;

        var ensuredAtUtc = DateTime.SpecifyKind(ensuredAt, DateTimeKind.Utc);
        var rql = $@"from '{CollectionName}' as e
where e.ClusterId = $clusterId and e.RecurringScheduleType = $staticType and e.StaticDefinitionId in ($staticDefinitionIds)
update {{
    if (this.StaticDefinitionLastEnsured == null || this.StaticDefinitionLastEnsured < $ensuredAt) {{
        this.StaticDefinitionLastEnsured = $ensuredAt;
    }}
}}";
        var operation = store.Operations.Send(new PatchByQueryOperation(new IndexQuery
        {
            Query = rql,
            QueryParameters = new Parameters
            {
                ["clusterId"] = ClusterConnConfig.ClusterId,
                ["staticType"] = RecurringScheduleType.Static,
                ["staticDefinitionIds"] = staticDefinitionIds.ToArray(),
                ["ensuredAt"] = ensuredAtUtc,
            },
        }, new QueryOperationOptions { AllowStale = true }));
        operation.WaitForCompletion();
    }

    public async Task<int> InactivateStaticDefinitionsOlderThanAsync(DateTime cutoff)
    {
        var cutoffUtc = DateTime.SpecifyKind(cutoff, DateTimeKind.Utc);
        var now = DateTime.UtcNow;

        var rql = $@"from '{CollectionName}' as e
where e.ClusterId = $clusterId and e.RecurringScheduleType = $staticType
update {{
    if ((this.StaticDefinitionLastEnsured == null || this.StaticDefinitionLastEnsured < $cutoff)
        && this.Status !== $inactive && this.Status !== $canceled && this.Status !== $completed) {{
        this.Status = $inactive;
        this.TerminatedAt = $now;
    }}
}}";
        var operation = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery
        {
            Query = rql,
            QueryParameters = new Parameters
            {
                ["clusterId"] = ClusterConnConfig.ClusterId,
                ["staticType"] = RecurringScheduleType.Static,
                ["cutoff"] = cutoffUtc,
                ["now"] = now,
                ["inactive"] = RecurringScheduleStatus.Inactive,
                ["canceled"] = RecurringScheduleStatus.Canceled,
                ["completed"] = RecurringScheduleStatus.Completed,
            },
        }, new QueryOperationOptions { AllowStale = true }));
        var result = await operation.WaitForCompletionAsync<BulkOperationResult>();
        return (int)result.Total;
    }

    // ==================== Purge / archive ====================

    public async Task<int> PurgeTerminatedAsync(DateTime cutoffUtc, int limit)
    {
        var ids = await QueryTerminatedIdsAsync(cutoffUtc, limit);
        if (ids.Count == 0) return 0;
        return await DeleteByIdsAsync(ids);
    }

    public async Task<IList<RecurringScheduleRawModel>> QueryTerminatedToPurgeAsync(DateTime cutoffUtc, int limit)
    {
        var cutoff = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Utc);
        using var session = store.OpenAsyncSession();
        var rql = $"from '{CollectionName}' as e where e.ClusterId = $clusterId and e.Status in ($terminatedStatuses) and e.TerminatedAt != null and e.TerminatedAt <= $cutoff order by e.TerminatedAt asc limit 0, $limit";
        var docs = await session.Advanced.AsyncRawQuery<RavenDbRecurringScheduleDocument>(rql)
            .AddParameter("clusterId", ClusterConnConfig.ClusterId)
            .AddParameter("terminatedStatuses", TerminatedStatuses)
            .AddParameter("cutoff", cutoff)
            .AddParameter("limit", limit)
            .ToListAsync();

        return docs.Select(doc => ToDomain(doc, session.Advanced.GetChangeVectorFor(doc))).ToList();
    }

    private async Task<IList<Guid>> QueryTerminatedIdsAsync(DateTime cutoffUtc, int limit)
    {
        var cutoff = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Utc);
        using var session = store.OpenAsyncSession();
        var rql = $"from '{CollectionName}' as e where e.ClusterId = $clusterId and e.Status in ($terminatedStatuses) and e.TerminatedAt != null and e.TerminatedAt <= $cutoff order by e.TerminatedAt asc select ScheduleId limit 0, $limit";
        var results = await session.Advanced.AsyncRawQuery<ScheduleIdProjection>(rql)
            .AddParameter("clusterId", ClusterConnConfig.ClusterId)
            .AddParameter("terminatedStatuses", TerminatedStatuses)
            .AddParameter("cutoff", cutoff)
            .AddParameter("limit", limit)
            .ToListAsync();

        return results.Select(r => r.ScheduleId).ToList();
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
        }

        return totalDeleted;
    }

    public async Task BulkInsertIfNotExistsAsync(IList<RecurringScheduleRawModel> schedules)
    {
        if (schedules.Count == 0) return;

        try
        {
            await InternalBulkInsertIfNotExistsAsync(schedules);
        }
        catch (ConcurrencyException ex)
        {
            logger.Warn(
                $"BulkInsertIfNotExistsAsync batch of {schedules.Count} recurring schedule(s) hit a change-vector conflict and fell back to partitions of {FallbackPartitionSize}.",
                JobMasterLogCategory.RecurringSchedule,
                exception: ex);

            var (_, failedSchedules) = await BulkInsertFallbackPerPartitionAsync(schedules);
            if (failedSchedules.Count > 0)
            {
                await BulkInsertFallbackPerRowAsync(failedSchedules);
            }
        }
    }

    // Pure "insert if not present" unit, no exception handling -- reused at every fallback granularity.
    private async Task<IList<RecurringScheduleRawModel>> InternalBulkInsertIfNotExistsAsync(IList<RecurringScheduleRawModel> schedules)
    {
        using var session = store.OpenAsyncSession();
        var ids = schedules.Select(s => DocumentId(s.Id)).ToArray();
        var existingDocs = await session.LoadAsync<RavenDbRecurringScheduleDocument>(ids);

        var pendingInserts = new List<(RecurringScheduleRawModel Model, RavenDbRecurringScheduleDocument Document)>();
        foreach (var schedule in schedules)
        {
            if (existingDocs[DocumentId(schedule.Id)] != null) continue; // Already exists -- leave untouched.

            var doc = ToDocument(schedule);
            await session.StoreAsync(doc, changeVector: string.Empty, id: DocumentId(schedule.Id));
            ApplyCollectionMetadata(session.Advanced.GetMetadataFor(doc));
            pendingInserts.Add((schedule, doc));
        }

        if (pendingInserts.Count == 0) return new List<RecurringScheduleRawModel>();

        await session.SaveChangesAsync();

        foreach (var (schedule, doc) in pendingInserts)
        {
            schedule.SetVersion(session.Advanced.GetChangeVectorFor(doc) ?? string.Empty);
        }

        return pendingInserts.Select(x => x.Model).ToList();
    }

    private async Task<(IList<RecurringScheduleRawModel> SucceededSchedules, IList<RecurringScheduleRawModel> FailedSchedules)> BulkInsertFallbackPerPartitionAsync(
        IList<RecurringScheduleRawModel> schedules)
    {
        var failedSchedules = new List<RecurringScheduleRawModel>();
        var succeededSchedules = new List<RecurringScheduleRawModel>();

        foreach (var partition in schedules.Partition(FallbackPartitionSize))
        {
            try
            {
                var saved = await InternalBulkInsertIfNotExistsAsync(partition.ToList());
                succeededSchedules.AddRange(saved);
            }
            catch (Exception ex)
            {
                logger.Error(
                    $"BulkInsertIfNotExistsAsync partition of {partition.Count} recurring schedule(s) failed and fell back to per-row retry.",
                    JobMasterLogCategory.RecurringSchedule,
                    exception: ex);
                failedSchedules.AddRange(partition);
            }
        }

        return (succeededSchedules, failedSchedules);
    }

    private async Task BulkInsertFallbackPerRowAsync(IList<RecurringScheduleRawModel> schedules)
    {
        await JobMasterParallelUtil.ForEachAsync(
            schedules,
            new ParallelOptions { MaxDegreeOfParallelism = PerRowSessionMaxDegreeOfParallelism },
            async (schedule, _) =>
            {
                try
                {
                    await InternalBulkInsertIfNotExistsAsync(new List<RecurringScheduleRawModel> { schedule });
                }
                catch (ConcurrencyException)
                {
                    // Already exists now -- leave untouched, matches "insert if not exists" semantics.
                }
            });
    }

    // ==================== WHERE builder ====================

    private (string Where, Dictionary<string, object?> Parameters) BuildWhereRql(RecurringScheduleQueryCriteria criteria, bool? isLocked)
    {
        var parameters = new Dictionary<string, object?> { ["clusterId"] = ClusterConnConfig.ClusterId };
        var where = new List<string> { "e.ClusterId = $clusterId" };

        if (criteria.Status.HasValue)
        {
            where.Add("e.Status = $status");
            parameters["status"] = criteria.Status.Value;
        }

        if (criteria.StartAfterTo.HasValue)
        {
            where.Add("(e.StartAfter <= $startAfterTo or e.StartAfter = null)");
            parameters["startAfterTo"] = DateTime.SpecifyKind(criteria.StartAfterTo.Value, DateTimeKind.Utc);
        }

        if (criteria.StartAfterFrom.HasValue)
        {
            where.Add("(e.StartAfter >= $startAfterFrom or e.StartAfter = null)");
            parameters["startAfterFrom"] = DateTime.SpecifyKind(criteria.StartAfterFrom.Value, DateTimeKind.Utc);
        }

        if (criteria.EndBeforeTo.HasValue)
        {
            where.Add("(e.EndBefore <= $endBeforeTo or e.EndBefore = null)");
            parameters["endBeforeTo"] = DateTime.SpecifyKind(criteria.EndBeforeTo.Value, DateTimeKind.Utc);
        }

        if (criteria.EndBeforeFrom.HasValue)
        {
            where.Add("(e.EndBefore >= $endBeforeFrom or e.EndBefore = null)");
            parameters["endBeforeFrom"] = DateTime.SpecifyKind(criteria.EndBeforeFrom.Value, DateTimeKind.Utc);
        }

        if (criteria.CoverageUntil.HasValue)
        {
            where.Add("(e.LastPlanCoverageUntil <= $coverageUntil or e.LastPlanCoverageUntil = null)");
            parameters["coverageUntil"] = DateTime.SpecifyKind(criteria.CoverageUntil.Value, DateTimeKind.Utc);
        }

        if (criteria.IsJobCancellationPending.HasValue)
        {
            where.Add("e.IsJobCancellationPending = $isJobCancellationPending");
            parameters["isJobCancellationPending"] = criteria.IsJobCancellationPending.Value;
        }

        if (criteria.CanceledOrInactive == true)
        {
            where.Add("e.Status in ($canceledOrInactiveStatuses)");
            parameters["canceledOrInactiveStatuses"] = new[] { RecurringScheduleStatus.Canceled, RecurringScheduleStatus.Inactive };
        }

        if (criteria.RecurringScheduleType.HasValue)
        {
            where.Add("e.RecurringScheduleType = $recurringScheduleType");
            parameters["recurringScheduleType"] = criteria.RecurringScheduleType.Value;
        }

        if (!string.IsNullOrEmpty(criteria.JobDefinitionId))
        {
            where.Add("e.JobDefinitionId = $jobDefinitionId");
            parameters["jobDefinitionId"] = criteria.JobDefinitionId;
        }

        if (!string.IsNullOrEmpty(criteria.ProfileId))
        {
            where.Add("e.ProfileId = $profileId");
            parameters["profileId"] = criteria.ProfileId;
        }

        if (!string.IsNullOrEmpty(criteria.WorkerLane))
        {
            where.Add("e.WorkerLane = $workerLane");
            parameters["workerLane"] = criteria.WorkerLane;
        }

        if (criteria.Ids is { Count: > 0 })
        {
            where.Add("e.ScheduleId in ($ids)");
            parameters["ids"] = criteria.Ids.ToArray();
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

    private RavenDbRecurringScheduleDocument ToDocument(RecurringScheduleRawModel model)
    {
        var doc = new RavenDbRecurringScheduleDocument
        {
            ScheduleId = model.Id,
            ClusterId = model.ClusterId,
            Expression = model.Expression,
            ExpressionTypeId = model.ExpressionTypeId,
            JobDefinitionId = model.JobDefinitionId,
            StaticDefinitionId = model.StaticDefinitionId,
            ProfileId = model.ProfileId,
            Status = model.Status,
            RecurringScheduleType = model.RecurringScheduleType,
            StaticDefinitionLastEnsured = model.StaticDefinitionLastEnsured.HasValue ? DateTime.SpecifyKind(model.StaticDefinitionLastEnsured.Value, DateTimeKind.Utc) : null,
            TerminatedAt = model.TerminatedAt.HasValue ? DateTime.SpecifyKind(model.TerminatedAt.Value, DateTimeKind.Utc) : null,
            MsgData = model.MsgData,
            Priority = model.Priority,
            MaxNumberOfRetries = model.MaxNumberOfRetries,
            TimeoutTicks = model.Timeout?.Ticks,
            BucketId = model.BucketId,
            AgentConnectionId = model.AgentConnectionId?.IdValue,
            AgentWorkerId = model.AgentWorkerId,
            PartitionLockId = model.PartitionLockId,
            HostId = model.HostId?.IdValue,
            HostDisplayName = model.HostId?.HostDisplayName,
            PartitionLockExpiresAt = model.PartitionLockExpiresAt.HasValue ? DateTime.SpecifyKind(model.PartitionLockExpiresAt.Value, DateTimeKind.Utc) : null,
            CreatedAt = DateTime.SpecifyKind(model.CreatedAt, DateTimeKind.Utc),
            StartAfter = model.StartAfter.HasValue ? DateTime.SpecifyKind(model.StartAfter.Value, DateTimeKind.Utc) : null,
            EndBefore = model.EndBefore.HasValue ? DateTime.SpecifyKind(model.EndBefore.Value, DateTimeKind.Utc) : null,
            LastPlanCoverageUntil = model.LastPlanCoverageUntil.HasValue ? DateTime.SpecifyKind(model.LastPlanCoverageUntil.Value, DateTimeKind.Utc) : null,
            LastExecutedPlan = model.LastExecutedPlan.HasValue ? DateTime.SpecifyKind(model.LastExecutedPlan.Value, DateTimeKind.Utc) : null,
            HasFailedOnLastPlanExecution = model.HasFailedOnLastPlanExecution,
            IsJobCancellationPending = model.IsJobCancellationPending,
            WorkerLane = model.WorkerLane,
        };

        ApplyMetadata(doc, model.Metadata);
        return doc;
    }

    // Same 4-bucket dispatch as RavenDbMasterJobsRepository.ApplyMetadata -- see its own comment for why.
    private static void ApplyMetadata(RavenDbRecurringScheduleDocument doc, string? metadataJson)
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

    private static RecurringScheduleRawModel ToDomain(RavenDbRecurringScheduleDocument doc, string? version)
    {
        var model = new RecurringScheduleRawModel(doc.ClusterId)
        {
            Id = doc.ScheduleId,
            Expression = doc.Expression,
            ExpressionTypeId = doc.ExpressionTypeId,
            JobDefinitionId = doc.JobDefinitionId,
            StaticDefinitionId = doc.StaticDefinitionId,
            ProfileId = doc.ProfileId,
            Status = doc.Status,
            RecurringScheduleType = doc.RecurringScheduleType,
            StaticDefinitionLastEnsured = doc.StaticDefinitionLastEnsured,
            TerminatedAt = doc.TerminatedAt,
            MsgData = string.IsNullOrEmpty(doc.MsgData) ? "{}" : doc.MsgData,
            Metadata = doc.MetadataJson,
            Priority = doc.Priority,
            MaxNumberOfRetries = doc.MaxNumberOfRetries,
            Timeout = doc.TimeoutTicks.HasValue ? TimeSpan.FromTicks(doc.TimeoutTicks.Value) : null,
            BucketId = doc.BucketId,
            AgentConnectionId = !string.IsNullOrEmpty(doc.AgentConnectionId) ? new AgentConnectionId(doc.AgentConnectionId!) : null,
            AgentWorkerId = doc.AgentWorkerId,
            PartitionLockId = doc.PartitionLockId,
            PartitionLockExpiresAt = doc.PartitionLockExpiresAt,
            CreatedAt = doc.CreatedAt,
            StartAfter = doc.StartAfter,
            EndBefore = doc.EndBefore,
            LastPlanCoverageUntil = doc.LastPlanCoverageUntil,
            LastExecutedPlan = doc.LastExecutedPlan,
            HasFailedOnLastPlanExecution = doc.HasFailedOnLastPlanExecution,
            IsJobCancellationPending = doc.IsJobCancellationPending,
            WorkerLane = doc.WorkerLane,
            HostId = !string.IsNullOrEmpty(doc.HostId) && !string.IsNullOrEmpty(doc.HostDisplayName)
                ? HostId.Recover(doc.HostDisplayName!, doc.HostId!)
                : null,
        };

        model.SetVersion(version ?? string.Empty);
        return model;
    }

    private void ApplyCollectionMetadata(Raven.Client.Documents.Session.IMetadataDictionary metadata)
    {
        metadata[Constants.Documents.Metadata.Collection] = CollectionName;
    }
}
