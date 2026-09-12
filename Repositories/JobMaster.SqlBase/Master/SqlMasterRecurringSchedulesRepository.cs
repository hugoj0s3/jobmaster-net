using System.Data;
using System.Linq.Expressions;
using System.Text;
using Dapper;
using JobMaster.SqlBase.Extensions;
using JobMaster.Sdk.Utils;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils.Extensions;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Models.RecurringSchedules;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.SqlBase.Master;

internal abstract class SqlMasterRecurringSchedulesRepository : JobMasterClusterAwareRepository, IMasterRecurringSchedulesRepository
{
    protected IDbConnectionManager connManager = null!;
    protected ISqlGenerator sql = null!;
    protected string connString = string.Empty;
    protected JobMasterConfigDictionary additionalConnConfig = null!;
    protected GenericRecordSqlUtil genericUtil = null!;

    protected SqlMasterRecurringSchedulesRepository(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IDbConnectionManager connManager) : base(clusterConnConfig)
    {
        this.connManager = connManager;
        sql = SqlGeneratorFactory.Get(this.MasterRepoTypeId);
        connString = clusterConnConfig.ConnectionString;
        additionalConnConfig = clusterConnConfig.AdditionalConnConfig;
        genericUtil = new GenericRecordSqlUtil(sql, additionalConnConfig, ClusterConnConfig.ClusterId);
    }

    public virtual async Task<IList<RecurringScheduleRawModel>> AcquireAndFetchAsync(
        RecurringScheduleQueryCriteria queryCriteria,
        Guid partitionLockId,
        DateTime expiresAtUtc)
    {
        if (partitionLockId == Guid.Empty) throw new ArgumentException("partitionLockId must be a valid GUID", nameof(partitionLockId));
        if (queryCriteria == null) throw new ArgumentNullException(nameof(queryCriteria));

        var nowUtcWithSkew = JobMasterConstants.NowUtcWithSkewTolerance();
        var expiresAtUtcKind = DateTime.SpecifyKind(expiresAtUtc, DateTimeKind.Utc);

        var unlockedGuard = $"({Col(x => x.PartitionLockId)} IS NULL OR {Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var (whereSql, args) = BuildWhere(queryCriteria);
            // Exclude actively locked records from the IDs subquery so CountLimit correctly
            // bounds acquirable candidates rather than including rows the UPDATE guard will reject.
            args["LockNowUtc"] = nowUtcWithSkew;
            var acquireWhereSql = whereSql + $" AND (s.{Col(x => x.PartitionLockId)} IS NULL OR s.{Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
            var needsMetadataJoin = queryCriteria.MetadataFilters is { Count: > 0 };
            var queryIdsSql = BuildQueryIdsToLockSql(acquireWhereSql, needsMetadataJoin, queryCriteria.CountLimit, queryCriteria.Offset, queryCriteria.SortBy);

            var updateSql = $@"
UPDATE {t} {UpdateToLockTableHint}
SET {Col(x => x.PartitionLockId)} = @PartitionLockId,
    {Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    {Col(x => x.Version)} = {sql.GenerateVersionSql()}
WHERE {Col(x => x.ClusterId)} = @ClusterId
  AND {Col(x => x.Id)} IN ({queryIdsSql})
  AND {unlockedGuard};";

            var args2 = new Dictionary<string, object?>(args);
            args2["LockNowUtc"] = nowUtcWithSkew;
            args2["LockExpiresAt"] = expiresAtUtcKind;
            args2["PartitionLockId"] = partitionLockId;
            if (needsMetadataJoin)
                args2["GroupId"] = MasterGenericRecordGroupIds.RecurringScheduleMetadata;

            var rowsAffected = await conn.ExecuteAsync(updateSql, args2, trans);

            trans.Commit();

            if (rowsAffected == 0) return new List<RecurringScheduleRawModel>();

            using var conn2 = await connManager.OpenAsync(connString, additionalConnConfig);
            return await QueryLockedSchedulesAsync(partitionLockId, nowUtcWithSkew, conn2);
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    public void Add(RecurringScheduleRawModel scheduleRaw)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var rec = SqlRecurringSchedulePersistenceConvertUtil.ToPersistence(scheduleRaw);
            if (rec.Metadata is not null)
            {
                var sqlEntry = genericUtil.MapToSqlEntry(rec.Metadata);
                var (insertSql, parameters) = genericUtil.BuildInsertEntrySql(sqlEntry);
                conn.Execute(insertSql, parameters, trans);

                var (insertValuesSql, paramRows) = genericUtil.BuildInsertEntryValuesSql(sqlEntry);
                conn.Execute(insertValuesSql, paramRows, trans);
            }

            // Generate initial version for new recurring schedule
            rec.Version = JobMasterRandomUtil.NewGuid4().ToString("N").ToLowerInvariant();

            var (cols, vals) = InsertColumnsAndParams();
            var sqlText = $"INSERT INTO {t} ({cols}) VALUES ({vals});";
            conn.Execute(sqlText, rec, trans);

            trans.Commit();
            
            // Update the in-memory model with the new version
            scheduleRaw.SetVersion(rec.Version);
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    public async Task AddAsync(RecurringScheduleRawModel scheduleRaw)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var rec = SqlRecurringSchedulePersistenceConvertUtil.ToPersistence(scheduleRaw);
            if (rec.Metadata is not null)
            {
                var sqlEntry = genericUtil.MapToSqlEntry(rec.Metadata);
                var (insertSql, parameters) = genericUtil.BuildInsertEntrySql(sqlEntry);
                await conn.ExecuteAsync(insertSql, parameters, trans);

                var (insertValuesSql, paramRows) = genericUtil.BuildInsertEntryValuesSql(sqlEntry);
                await conn.ExecuteAsync(insertValuesSql, paramRows, trans);
            }

            // Generate initial version for new recurring schedule
            rec.Version = JobMasterRandomUtil.NewGuid4().ToString("N").ToLowerInvariant();

            var (cols, vals) = InsertColumnsAndParams();
            var sqlText = $"INSERT INTO {t} ({cols}) VALUES ({vals});";
            await conn.ExecuteAsync(sqlText, rec, trans);

            trans.Commit();
            
            // Update the in-memory model with the new version
            scheduleRaw.SetVersion(rec.Version);
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    public bool Exists(Guid recurringScheduleId)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var sqlText = $"SELECT 1 FROM {TableName()} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id";
        return conn.ExecuteScalar<bool>(sqlText, new { ClusterId = ClusterConnConfig.ClusterId, Id = recurringScheduleId });
    }

    public async Task<bool> ExistsAsync(Guid recurringScheduleId)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var sqlText = $"SELECT 1 FROM {TableName()} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id";
        return await conn.ExecuteScalarAsync<bool>(sqlText, new { ClusterId = ClusterConnConfig.ClusterId, Id = recurringScheduleId });
    }

    public void Update(RecurringScheduleRawModel scheduleRaw)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var rec = SqlRecurringSchedulePersistenceConvertUtil.ToPersistence(scheduleRaw);
            var expectedVersion = rec.Version;
            rec.Version = JobMasterRandomUtil.NewGuid4().ToString("N").ToLowerInvariant();

            var t = TableName();
            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = conn.Execute(BuildUpdateSql(), dp, trans);

            if (rowsAffected == 0)
            {
                var exists = conn.ExecuteScalar<bool>(
                    $"SELECT 1 FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id",
                    new { rec.ClusterId, rec.Id }, trans);
                if (exists)
                    throw new JobMasterVersionConflictException(scheduleRaw.Id, "RecurringSchedule", expectedVersion);
            }

            trans.Commit();
            scheduleRaw.SetVersion(rec.Version);
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    public async Task UpdateAsync(RecurringScheduleRawModel scheduleRaw)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var rec = SqlRecurringSchedulePersistenceConvertUtil.ToPersistence(scheduleRaw);
            var expectedVersion = rec.Version;
            rec.Version = JobMasterRandomUtil.NewGuid4().ToString("N").ToLowerInvariant();

            var t = TableName();
            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = await conn.ExecuteAsync(BuildUpdateSql(), dp, trans);

            if (rowsAffected == 0)
            {
                var exists = await conn.ExecuteScalarAsync<bool>(
                    $"SELECT 1 FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id",
                    new { rec.ClusterId, rec.Id }, trans);
                if (exists)
                    throw new JobMasterVersionConflictException(scheduleRaw.Id, "RecurringSchedule", expectedVersion);
            }

            trans.Commit();
            scheduleRaw.SetVersion(rec.Version);
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    public IList<RecurringScheduleRawModel> Query(RecurringScheduleQueryCriteria queryCriteria)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var (sqlText, args) = BuildQuerySql(queryCriteria);
        var linearRows = conn.Query<SqlRecurringSchedulePersistenceRecordLinearDto>(sqlText, args).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(SqlRecurringSchedulePersistenceConvertUtil.FromPersistence).ToList();
    }

    public async Task<IList<RecurringScheduleRawModel>> QueryAsync(RecurringScheduleQueryCriteria queryCriteria)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var (sqlText, args) = BuildQuerySql(queryCriteria);
        var linearRows = (await conn.QueryAsync<SqlRecurringSchedulePersistenceRecordLinearDto>(sqlText, args)).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(SqlRecurringSchedulePersistenceConvertUtil.FromPersistence).ToList();
    }

    public RecurringScheduleRawModel? Get(Guid recurringScheduleId)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var (sqlText, args) = BuildGetSql(recurringScheduleId);
        var linearRows = conn.Query<SqlRecurringSchedulePersistenceRecordLinearDto>(sqlText, args).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(SqlRecurringSchedulePersistenceConvertUtil.FromPersistence).SingleOrDefault();
    }

    public async Task<RecurringScheduleRawModel?> GetAsync(Guid recurringScheduleId)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var (sqlText, args) = BuildGetSql(recurringScheduleId);
        var linearRows = (await conn.QueryAsync<SqlRecurringSchedulePersistenceRecordLinearDto>(sqlText, args)).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(SqlRecurringSchedulePersistenceConvertUtil.FromPersistence).SingleOrDefault();
    }

    public RecurringScheduleRawModel? GetByStaticId(string staticId)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var (sqlText, args) = BuildGetByStaticIdSql(staticId);
        var linearRows = conn.Query<SqlRecurringSchedulePersistenceRecordLinearDto>(sqlText, args).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(SqlRecurringSchedulePersistenceConvertUtil.FromPersistence).SingleOrDefault();
    }

    public async Task<int> InactivateStaticDefinitionsOlderThanAsync(DateTime cutoff)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var now = DateTime.UtcNow;
            var cClusterId = Col(x => x.ClusterId);
            var cType = Col(x => x.RecurringScheduleType);
            var cStatus = Col(x => x.Status);
            var cLastEnsured = Col(x => x.StaticDefinitionLastEnsured);
            var cTerminated = Col(x => x.TerminatedAt);

            var sqlText = $@"UPDATE {t}
SET {cStatus} = @Inactive,
    {cTerminated} = @Now
WHERE {cClusterId} = @ClusterId
  AND {cType} = @StaticType
  AND ({cLastEnsured} IS NULL OR {cLastEnsured} < @Cutoff)
  AND {cStatus} <> @Inactive
  AND {cStatus} <> @Canceled
  AND {cStatus} <> @Completed";

            var affected = await conn.ExecuteAsync(sqlText, new
            {
                ClusterId = ClusterConnConfig.ClusterId,
                StaticType = (int)RecurringScheduleType.Static,
                Cutoff = cutoff,
                Now = now,
                Inactive = (int)RecurringScheduleStatus.Inactive,
                Canceled = (int)RecurringScheduleStatus.Canceled,
                Completed = (int)RecurringScheduleStatus.Completed
            }, tx);

            tx.Commit();
            return affected;
        }
        catch
        {
            tx.SafeRollback();
            throw;
        }
    }


    public void BulkUpdateStaticDefinitionLastEnsuredByStaticIds(IList<string> staticDefinitionIds, DateTime ensuredAt)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var colEnsured = Col(x => x.StaticDefinitionLastEnsured);
            var colStaticId = Col(x => x.StaticDefinitionId);
            // Update only when NULL or older than incoming timestamp
            var sqlText = $@"UPDATE {t}
SET {colEnsured} = @EnsuredAt
WHERE {this.sql.InClauseFor(colStaticId, "@StaticDefinitionIds")} 
  AND {Col(x => x.RecurringScheduleType)} = @RecurringScheduleType
  AND {Col(x => x.ClusterId)} = @ClusterId
  AND ({colEnsured} IS NULL OR {colEnsured} < @EnsuredAt)";
            
            conn.Execute(sqlText, new
            {
                StaticDefinitionIds = staticDefinitionIds, 
                EnsuredAt = ensuredAt, 
                ClusterId = ClusterConnConfig.ClusterId, 
                RecurringScheduleType = (int)RecurringScheduleType.Static
            }, trans);
            
            trans.Commit();
        }
        catch (Exception)
        {
            trans.SafeRollback();
            throw;
        }
    }

    public long Count(RecurringScheduleQueryCriteria queryCriteria)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var (whereSql, args) = BuildWhere(queryCriteria);
        var t = TableName();
        var sqlText = $"SELECT COUNT(*) FROM {t} s {whereSql}";
        return conn.ExecuteScalar<long>(sqlText, args);
    }

    public async Task<long> ProbeCountForAcquireAsync(RecurringScheduleQueryCriteria queryCriteria)
    {
        if (queryCriteria.MetadataFilters.Count > 0)
            throw new NotSupportedException("ProbeCountForAcquire does not support MetadataFilters.");

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var (whereSql, args) = BuildWhere(queryCriteria, isLocked: false);
        var t = TableName();
        var sqlText = $"SELECT COUNT(*) FROM {t} s {whereSql}";
        return await conn.ExecuteScalarAsync<long>(sqlText, args);
    }

    public async Task<int> PurgeTerminatedAsync(DateTime cutoffUtc, int limit)
    {
        if (limit <= 0) throw new ArgumentException("limit must be > 0", nameof(limit));

        var t = TableName();
        var cId = Col(x => x.Id);
        var cStatus = Col(x => x.Status);
        var cTerminatedAt = Col(x => x.TerminatedAt);

        var selectSql = new StringBuilder($@"SELECT {cId} FROM {t}
WHERE {Col(x => x.ClusterId)} = @ClusterId
  AND {cStatus} IN (@Inactive, @Canceled, @Completed)
  AND {cTerminatedAt} <= @CutoffUtc
ORDER BY {cTerminatedAt} ASC, {cId} ASC");
        selectSql.Append('\n');
        selectSql.Append(sql.OffsetQueryFor(limit, 0));

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var ids = (await conn.QueryAsync<Guid>(selectSql.ToString(), new
        {
            ClusterId = ClusterConnConfig.ClusterId,
            CutoffUtc = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Utc),
            Inactive = (int)RecurringScheduleStatus.Inactive,
            Canceled = (int)RecurringScheduleStatus.Canceled,
            Completed = (int)RecurringScheduleStatus.Completed
        })).ToList();

        if (ids.Count == 0) return 0;

        return await DeleteByIdsAsync(ids);
    }

    public async Task<IList<RecurringScheduleRawModel>> QueryTerminatedToPurgeAsync(DateTime cutoffUtc, int limit)
    {
        if (limit <= 0) throw new ArgumentException("limit must be > 0", nameof(limit));

        var t = TableName();
        var selectCols = SelectProjection();
        var cId = Col(x => x.Id);
        var cClusterId = Col(x => x.ClusterId);
        var cStatus = Col(x => x.Status);
        var cTerminatedAt = Col(x => x.TerminatedAt);

        var whereSql = $@"WHERE s.{cClusterId} = @ClusterId
  AND s.{cStatus} IN (@Inactive, @Canceled, @Completed)
  AND s.{cTerminatedAt} <= @CutoffUtc";
        var order = $"ORDER BY s.{cTerminatedAt} ASC, s.{cId} ASC";
        var offsetClause = sql.OffsetQueryFor(limit, 0);

        var queryText = $@"
WITH schedules_page AS (
    SELECT s.*
    FROM {t} s
    {whereSql}
    {order}
    {offsetClause}
)
SELECT {selectCols}
FROM schedules_page s
{order}";

        var args = new
        {
            ClusterId = ClusterConnConfig.ClusterId,
            CutoffUtc = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Utc),
            Inactive = (int)RecurringScheduleStatus.Inactive,
            Canceled = (int)RecurringScheduleStatus.Canceled,
            Completed = (int)RecurringScheduleStatus.Completed,
        };

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var linearRows = (await conn.QueryAsync<SqlRecurringSchedulePersistenceRecordLinearDto>(queryText, args)).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(SqlRecurringSchedulePersistenceConvertUtil.FromPersistence).ToList();
    }

    public async Task<int> DeleteByIdsAsync(IList<Guid> ids)
    {
        if (ids.Count == 0) return 0;

        var t = TableName();
        var cId = Col(x => x.Id);
        var cClusterId = Col(x => x.ClusterId);

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var affected = 0;
            var maxBatchSize = OperationThrottlerSettingsTemplateFactory.GetMasterMaxBatchSize(ClusterConnConfig.RepositoryTypeId);
            foreach (var idsPartition in ids.Partition(maxBatchSize).ToList())
            {
                var inClause = sql.InClauseFor(cId, "@Ids");
                var deleteSql = $"DELETE FROM {t} WHERE {cClusterId} = @ClusterId AND {inClause}";
                affected += await conn.ExecuteAsync(deleteSql, new { ClusterId = ClusterConnConfig.ClusterId, Ids = idsPartition }, tx);

                // Delete associated metadata
                var metadataUniqueIds = idsPartition.Select(id => GenericRecordEntry.UniqueId(this.ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.RecurringScheduleMetadata, id)).ToList();
                var deleteMetadataValuesSql = genericUtil.BuildDeleteValuesMultipleSql(MasterGenericRecordGroupIds.RecurringScheduleMetadata, "@metadataUniqueIds");
                await conn.ExecuteAsync(deleteMetadataValuesSql, new { ClusterId = ClusterConnConfig.ClusterId, metadataUniqueIds }, tx);

                var deleteMetadataEntrySql = genericUtil.BuildDeleteEntryMultipleSql(MasterGenericRecordGroupIds.RecurringScheduleMetadata, "@metadataUniqueIds");
                await conn.ExecuteAsync(deleteMetadataEntrySql, new { ClusterId = ClusterConnConfig.ClusterId, metadataUniqueIds }, tx);
            }

            tx.Commit();
            return affected;
        }
        catch
        {
            tx.SafeRollback();
            throw;
        }
    }

    public virtual async Task BulkInsertIfNotExistsAsync(IList<RecurringScheduleRawModel> schedules)
    {
        if (schedules.Count == 0) return;

        var t = TableName();
        var maxBatch = OperationThrottlerSettingsTemplateFactory.GetMasterMaxBatchSize(ClusterConnConfig.RepositoryTypeId);

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            for (var offset = 0; offset < schedules.Count; offset += maxBatch)
            {
                var count = Math.Min(maxBatch, schedules.Count - offset);
                var batch = schedules.Skip(offset).Take(count).ToList();

                var (existsSql, existsParams) = genericUtil.BuildQueryExistingIdsSql(t, Col(x => x.Id), batch.Select(s => s.Id));
                var existingIds = new HashSet<Guid>(await conn.QueryAsync<Guid>(existsSql, existsParams, tx));
                var newSchedules = batch.Where(s => !existingIds.Contains(s.Id)).ToList();

                var (sqlText, dynParams) = BuildBulkInsertIfNotExistsSql(t, schedules, offset, count);
                await conn.ExecuteAsync(sqlText, dynParams, tx);

                if (newSchedules.Count > 0)
                {
                    var newEntries = newSchedules
                        .Select(s => genericUtil.MapToSqlEntry(SqlRecurringSchedulePersistenceConvertUtil.ToPersistence(s).Metadata!))
                        .ToList();

                    var entriesSql = genericUtil.BuildBulkInsertEntriesSql(newEntries);
                    if (entriesSql is not null)
                    {
                        await conn.ExecuteAsync(entriesSql.Value.sqlText, entriesSql.Value.dynParams, tx);
                    }

                    var valuesSql = genericUtil.BuildBulkInsertEntryValuesSql(newEntries);
                    if (valuesSql is not null)
                    {
                        await conn.ExecuteAsync(valuesSql.Value.sqlText, valuesSql.Value.dynParams, tx);
                    }
                }
            }
            tx.Commit();
        }
        catch
        {
            tx.SafeRollback();
            throw;
        }
    }

    // ANSI-portable "insert if not exists": each row is its own guarded SELECT, unioned together.
    // Marked virtual so providers can later override with a dialect-specific upsert
    // (ON CONFLICT DO NOTHING / INSERT IGNORE / MERGE) for better performance.
    protected virtual (string sqlText, DynamicParameters dynParams) BuildBulkInsertIfNotExistsSql(
        string t, IList<RecurringScheduleRawModel> schedules, int offset, int count)
    {
        var (cols, _) = InsertColumnsAndParams();
        var cId = Col(x => x.Id);
        var cClusterId = Col(x => x.ClusterId);

        var dynParams = new DynamicParameters();
        var selects = new List<string>();

        for (var i = 0; i < count; i++)
        {
            var schedule = schedules[offset + i];
            var rec = SqlRecurringSchedulePersistenceConvertUtil.ToPersistence(schedule);
            rec.Version = JobMasterRandomUtil.NewGuid4().ToString("N").ToLowerInvariant();

            selects.Add($@"SELECT @ClusterId_{i}, @Id_{i}, @Expression_{i}, @ExpressionTypeId_{i}, @JobDefinitionId_{i}, @StaticDefinitionId_{i}, @ProfileId_{i}, @Status_{i}, @RecurringScheduleType_{i}, @StaticDefinitionLastEnsured_{i}, @TerminatedAt_{i}, @MsgData_{i}, @MetadataJson_{i}, @Priority_{i}, @MaxNumberOfRetries_{i}, @TimeoutTicks_{i}, @BucketId_{i}, @AgentConnectionId_{i}, @AgentWorkerId_{i}, @PartitionLockId_{i}, @HostId_{i}, @HostDisplayName_{i}, @PartitionLockExpiresAt_{i}, @CreatedAt_{i}, @StartAfter_{i}, @EndBefore_{i}, @LastPlanCoverageUntil_{i}, @LastExecutedPlan_{i}, @HasFailedOnLastPlanExecution_{i}, @IsJobCancellationPending_{i}, @WorkerLane_{i}, @Version_{i}
WHERE NOT EXISTS (SELECT 1 FROM {t} WHERE {cClusterId} = @ExistsClusterId_{i} AND {cId} = @ExistsId_{i})");

            dynParams.Add($"ClusterId_{i}", rec.ClusterId, DbType.String);
            dynParams.Add($"Id_{i}", rec.Id, DbType.Guid);
            dynParams.Add($"Expression_{i}", rec.Expression, DbType.String);
            dynParams.Add($"ExpressionTypeId_{i}", rec.ExpressionTypeId, DbType.String);
            dynParams.Add($"JobDefinitionId_{i}", rec.JobDefinitionId, DbType.String);
            dynParams.Add($"StaticDefinitionId_{i}", rec.StaticDefinitionId, DbType.String);
            dynParams.Add($"ProfileId_{i}", rec.ProfileId, DbType.String);
            dynParams.Add($"Status_{i}", rec.Status, DbType.Int32);
            dynParams.Add($"RecurringScheduleType_{i}", rec.RecurringScheduleType, DbType.Int32);
            dynParams.Add($"StaticDefinitionLastEnsured_{i}", rec.StaticDefinitionLastEnsured, DbType.DateTime);
            dynParams.Add($"TerminatedAt_{i}", rec.TerminatedAt, DbType.DateTime);
            dynParams.Add($"MsgData_{i}", rec.MsgData, DbType.String);
            dynParams.Add($"MetadataJson_{i}", rec.MetadataJson, DbType.String);
            dynParams.Add($"Priority_{i}", rec.Priority, DbType.Int32);
            dynParams.Add($"MaxNumberOfRetries_{i}", rec.MaxNumberOfRetries, DbType.Int32);
            dynParams.Add($"TimeoutTicks_{i}", rec.TimeoutTicks, DbType.Int64);
            dynParams.Add($"BucketId_{i}", rec.BucketId, DbType.String);
            dynParams.Add($"AgentConnectionId_{i}", rec.AgentConnectionId, DbType.String);
            dynParams.Add($"AgentWorkerId_{i}", rec.AgentWorkerId, DbType.String);
            dynParams.Add($"PartitionLockId_{i}", rec.PartitionLockId, DbType.Guid);
            dynParams.Add($"HostId_{i}", rec.HostId, DbType.String);
            dynParams.Add($"HostDisplayName_{i}", rec.HostDisplayName, DbType.String);
            dynParams.Add($"PartitionLockExpiresAt_{i}", rec.PartitionLockExpiresAt, DbType.DateTime);
            dynParams.Add($"CreatedAt_{i}", rec.CreatedAt, DbType.DateTime);
            dynParams.Add($"StartAfter_{i}", rec.StartAfter, DbType.DateTime);
            dynParams.Add($"EndBefore_{i}", rec.EndBefore, DbType.DateTime);
            dynParams.Add($"LastPlanCoverageUntil_{i}", rec.LastPlanCoverageUntil, DbType.DateTime);
            dynParams.Add($"LastExecutedPlan_{i}", rec.LastExecutedPlan, DbType.DateTime);
            dynParams.Add($"HasFailedOnLastPlanExecution_{i}", rec.HasFailedOnLastPlanExecution, DbType.Boolean);
            dynParams.Add($"IsJobCancellationPending_{i}", rec.IsJobCancellationPending, DbType.Boolean);
            dynParams.Add($"WorkerLane_{i}", rec.WorkerLane, DbType.String);
            dynParams.Add($"Version_{i}", rec.Version, DbType.String);
            dynParams.Add($"ExistsClusterId_{i}", rec.ClusterId, DbType.String);
            dynParams.Add($"ExistsId_{i}", rec.Id, DbType.Guid);
        }

        var sqlText = $"INSERT INTO {t} ({cols})\n{string.Join("\nUNION ALL\n", selects)};";
        return (sqlText, dynParams);
    }

    protected virtual string UpdateToLockTableHint => string.Empty;

    protected virtual string BuildQueryIdsToLockSql(
        string whereSql,
        bool needsMetadataJoin,
        int countLimit,
        int offset,
        SortByCriteria? sortByCriteria)
    {
        var metadataJoin = string.Empty;
        if (needsMetadataJoin)
        {
            metadataJoin = $@"
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata)} e ON
    e.{Col(x => x.EntryIdGuid)} = s.{Col(x => x.Id)} AND
    e.{Col(x => x.GroupId)} = @GroupId ";
        }

        var sb = new StringBuilder();
        sb.Append($@"
SELECT {Col(x => x.Id)}
FROM {TableName()} s
{metadataJoin}
{whereSql}");
        var sortBy = SqlOrderByUtil.BuildOrderByClause(sortByCriteria, "s", $" ORDER BY s.{Col(x => x.LastPlanCoverageUntil)} DESC");
        sb.Append(sortBy);
        sb.Append('\n');
        sb.Append(sql.OffsetQueryFor(countLimit, offset));
        return sb.ToString();
    }

    protected async Task<IList<RecurringScheduleRawModel>> QueryLockedSchedulesAsync(
        Guid partitionLockId,
        DateTime nowUtcWithSkew,
        IDbConnection conn2)
    {
        var selectCols = SelectProjection();
        var t = TableName();
        var cClusterId = Col(x => x.ClusterId);
        var cPartitionLockId = Col(x => x.PartitionLockId);
        var cPartitionLockExpiresAt = Col(x => x.PartitionLockExpiresAt);

        var sqlText = $@"
SELECT {selectCols}
FROM {t} s
WHERE s.{cClusterId} = @ClusterId
  AND s.{cPartitionLockId} = @PartitionLockId
  AND s.{cPartitionLockExpiresAt} > @NowUtcWithSkew";

        var args = new Dictionary<string, object?>
        {
            { "ClusterId", ClusterConnConfig.ClusterId },
            { "PartitionLockId", partitionLockId },
            { "NowUtcWithSkew", nowUtcWithSkew }
        };

        var linearRows = (await conn2.QueryAsync<SqlRecurringSchedulePersistenceRecordLinearDto>(sqlText, args)).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(SqlRecurringSchedulePersistenceConvertUtil.FromPersistence).ToList();
    }

    // SQL builders
    protected (string, object) BuildGetSql(Guid id)
    {
        var t = TableName();
        var selectCols = SelectProjection();
        var sqlText = $@"
SELECT {selectCols}
FROM {t} s
WHERE s.{Col(x => x.ClusterId)} = @ClusterId AND s.{Col(x => x.Id)} = @Id";
        var args = new { ClusterId = ClusterConnConfig.ClusterId, Id = id };
        return (sqlText, args);
    }

    protected (string, object) BuildQuerySql(RecurringScheduleQueryCriteria c)
    {
        var t = TableName();
        var selectCols = SelectProjection();

        if (c.CountLimit < 0)
            throw new ArgumentOutOfRangeException(nameof(c.CountLimit), c.CountLimit, "CountLimit must be >= 0");
        if (c.Offset < 0)
            throw new ArgumentOutOfRangeException(nameof(c.Offset), c.Offset, "Offset must be >= 0");

        var (whereSql, args) = BuildWhere(c);
        var defaultOrderByClause = $" ORDER BY s.{Col(x => x.LastPlanCoverageUntil)} DESC, s.{Col(x => x.CreatedAt)} ASC";
        var order = SqlOrderByUtil.BuildOrderByClause(c.SortBy, "s", defaultOrderByClause);

        // genericUtil.BuildWhereClause's EXISTS subquery (folded into whereSql by BuildWhere) only
        // defines its own value-table alias -- it correlates against alias "e", which must be joined
        // in this outer query. Only join it when MetadataFilters is actually in play; otherwise every
        // plain query pays for a join it never uses.
        var needsMetadataJoin = c.MetadataFilters is { Count: > 0 };
        var concatedArgs = args;
        var metadataJoin = string.Empty;
        if (needsMetadataJoin)
        {
            concatedArgs = args.Concat(new Dictionary<string, object?> { { "GroupId", MasterGenericRecordGroupIds.RecurringScheduleMetadata } })
                .ToDictionary(x => x.Key, x => x.Value);
            metadataJoin = $"LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = s.{Col(x => x.Id)} AND e.{Col(x => x.GroupId)} = @GroupId";
        }

        if (c.CountLimit > 0)
        {
            var offsetClause = sql.OffsetQueryFor(c.CountLimit, c.Offset);

            var queryText = $@"
WITH schedules_page AS (
    SELECT s.*
    FROM {t} s
    {metadataJoin}
    {whereSql}
    {order}
    {offsetClause}
)
SELECT {selectCols} FROM schedules_page s
{order}";

            return (queryText, concatedArgs);
        }

        var sb = new StringBuilder();
        sb.Append($@"
SELECT {selectCols} FROM {t} s
{metadataJoin}
{whereSql}
{order}");

        return (sb.ToString(), concatedArgs);
    }

    protected (string, Dictionary<string, object?>) BuildWhere(RecurringScheduleQueryCriteria c, bool? isLocked = null)
    {
        var where = new List<string> { $"s.{Col(x => x.ClusterId)} = @ClusterId" };
        var args = new Dictionary<string, object?>();
        args.Add("ClusterId", ClusterConnConfig.ClusterId);

        if (c.Status.HasValue)
        {
            where.Add($"s.{Col(x => x.Status)} = @Status");
            args.Add("Status", (int)c.Status.Value);
        }

        if (isLocked.HasValue)
        {
            where.Add(isLocked.Value
                ? $"(s.{Col(x => x.PartitionLockId)} IS NOT NULL AND s.{Col(x => x.PartitionLockExpiresAt)} > @NowUtc)"
                : $"(s.{Col(x => x.PartitionLockId)} IS NULL OR s.{Col(x => x.PartitionLockExpiresAt)} < @NowUtcWithSkewPadding)");
            args.Add("NowUtc", DateTime.UtcNow);
            args.Add("NowUtcWithSkewPadding", JobMasterConstants.NowUtcWithSkewTolerance());
        }

        if (c.StartAfterTo.HasValue)
        {
            where.Add($"(s.{Col(x => x.StartAfter)} <= @StartAfterTo OR s.{Col(x => x.StartAfter)} IS NULL)");
            args.Add("StartAfterTo", c.StartAfterTo.Value);
        }
        
        if (c.StartAfterFrom.HasValue)
        {
            where.Add($"(s.{Col(x => x.StartAfter)} >= @StartAfterFrom OR s.{Col(x => x.StartAfter)} IS NULL)");
            args.Add("StartAfterFrom", c.StartAfterFrom.Value);
        }
        
        if (c.EndBeforeTo.HasValue)
        {
            where.Add($"(s.{Col(x => x.EndBefore)} <= @EndBeforeTo OR s.{Col(x => x.EndBefore)} IS NULL)");
            args.Add("EndBeforeTo", c.EndBeforeTo.Value);
        }
        
        if (c.EndBeforeFrom.HasValue)
        {
            where.Add($"(s.{Col(x => x.EndBefore)} >= @EndBeforeFrom OR s.{Col(x => x.EndBefore)} IS NULL)");
            args.Add("EndBeforeFrom", c.EndBeforeFrom.Value);
        }

        if (c.CoverageUntil.HasValue)
        {
            where.Add($"(s.{Col(x => x.LastPlanCoverageUntil)} <= @CoverageUntil OR s.{Col(x => x.LastPlanCoverageUntil)} IS NULL)");
            args.Add("CoverageUntil", c.CoverageUntil.Value);
        }
        
        if (c.IsJobCancellationPending.HasValue)
        {
            where.Add($"s.{Col(x => x.IsJobCancellationPending)} = @IsJobCancellationPending");
            args.Add("IsJobCancellationPending", c.IsJobCancellationPending.Value);
        }
        
        if (c.CanceledOrInactive.HasValue)
        {
            where.Add($"s.{Col(x => x.Status)} IN (@CancelStatus, @Inactive)");
            args.Add("CancelStatus", (int)RecurringScheduleStatus.Canceled);
            args.Add("Inactive", (int)RecurringScheduleStatus.Inactive);
        }
        
        if (!string.IsNullOrEmpty(c.JobDefinitionId))
        {
            where.Add($"s.{Col(x => x.JobDefinitionId)} = @JobDefinitionId");
            args.Add("JobDefinitionId", c.JobDefinitionId);
        }

        if (!string.IsNullOrEmpty(c.ProfileId))
        {
            where.Add($"s.{Col(x => x.ProfileId)} = @ProfileId");
            args.Add("ProfileId", c.ProfileId);
        }
        
        if (!string.IsNullOrEmpty(c.WorkerLane))
        {
            where.Add($"s.{Col(x => x.WorkerLane)} = @WorkerLane");
            args.Add("WorkerLane", c.WorkerLane);
        }
        
        if (c.Ids != null && c.Ids.Count > 0)
        {
            where.Add(sql.InClauseFor($"s.{Col(x => x.Id)}", "@Ids"));
            args.Add("Ids", c.Ids);
        }
        
        if (c.RecurringScheduleType.HasValue)
        {
            where.Add($"s.{Col(x => x.RecurringScheduleType)} = @RecurringScheduleType");
            args.Add("RecurringScheduleType", (int)c.RecurringScheduleType.Value);
        }
        
        var metadataFilter = genericUtil.BuildWhereClause(c.MetadataFilters, "e", "existsV", args, MasterGenericRecordGroupIds.RecurringScheduleMetadata);
        if (!string.IsNullOrEmpty(metadataFilter))
        {
            where.Add(metadataFilter);
        }

        var whereSql = "WHERE " + string.Join(" AND ", where);
        return (whereSql, args);
    }

    protected string TableName()
    {
        return sql.TableNameFor<RecurringSchedule>(additionalConnConfig);
    }

    // No more generic-record entry/value LEFT JOIN columns here -- Metadata is read straight off
    // MetadataJson (see SqlRecurringSchedulePersistenceRecord.MetadataJson). The entry/value tables
    // remain the source of truth for MetadataFilters querying only.
    protected string SelectProjection(string scheduleAlias = "s")
    {
        return string.Join(", ", new[]
        {
            $"{scheduleAlias}.{Col(x => x.ClusterId)}",
            $"{scheduleAlias}.{Col(x => x.Id)}",
            $"{scheduleAlias}.{Col(x => x.Expression)}",
            $"{scheduleAlias}.{Col(x => x.ExpressionTypeId)}",
            $"{scheduleAlias}.{Col(x => x.JobDefinitionId)}",
            $"{scheduleAlias}.{Col(x => x.StaticDefinitionId)}",
            $"{scheduleAlias}.{Col(x => x.ProfileId)}",
            $"{scheduleAlias}.{Col(x => x.Status)}",
            $"{scheduleAlias}.{Col(x => x.RecurringScheduleType)}",
            $"{scheduleAlias}.{Col(x => x.StaticDefinitionLastEnsured)}",
            $"{scheduleAlias}.{Col(x => x.TerminatedAt)}",
            $"{scheduleAlias}.{Col(x => x.MsgData)}",
            $"{scheduleAlias}.{Col(x => x.MetadataJson)}",
            $"{scheduleAlias}.{Col(x => x.Priority)}",
            $"{scheduleAlias}.{Col(x => x.MaxNumberOfRetries)}",
            $"{scheduleAlias}.{Col(x => x.TimeoutTicks)}",
            $"{scheduleAlias}.{Col(x => x.BucketId)}",
            $"{scheduleAlias}.{Col(x => x.AgentConnectionId)}",
            $"{scheduleAlias}.{Col(x => x.AgentWorkerId)}",
            $"{scheduleAlias}.{Col(x => x.PartitionLockId)}",
            $"{scheduleAlias}.{Col(x => x.HostId)}",
            $"{scheduleAlias}.{Col(x => x.HostDisplayName)}",
            $"{scheduleAlias}.{Col(x => x.PartitionLockExpiresAt)}",
            $"{scheduleAlias}.{Col(x => x.CreatedAt)}",
            $"{scheduleAlias}.{Col(x => x.StartAfter)}",
            $"{scheduleAlias}.{Col(x => x.EndBefore)}",
            $"{scheduleAlias}.{Col(x => x.LastPlanCoverageUntil)}",
            $"{scheduleAlias}.{Col(x => x.LastExecutedPlan)}",
            $"{scheduleAlias}.{Col(x => x.HasFailedOnLastPlanExecution)}",
            $"{scheduleAlias}.{Col(x => x.IsJobCancellationPending)}",
            $"{scheduleAlias}.{Col(x => x.WorkerLane)}",
            $"{scheduleAlias}.{Col(x => x.Version)}",
        });
    }

    protected (string Columns, string ValuesParams) InsertColumnsAndParams()
    {
        var cols = new[]
        {
            Col(x => x.ClusterId), Col(x => x.Id), Col(x => x.Expression), Col(x => x.ExpressionTypeId),
            Col(x => x.JobDefinitionId), Col(x => x.StaticDefinitionId), Col(x => x.ProfileId), Col(x => x.Status), Col(x => x.RecurringScheduleType),
            Col(x => x.StaticDefinitionLastEnsured), Col(x => x.TerminatedAt), Col(x => x.MsgData), Col(x => x.MetadataJson), Col(x => x.Priority), Col(x => x.MaxNumberOfRetries),
            Col(x => x.TimeoutTicks), Col(x => x.BucketId), Col(x => x.AgentConnectionId), Col(x => x.AgentWorkerId),
            Col(x => x.PartitionLockId), Col(x => x.HostId), Col(x => x.HostDisplayName), Col(x => x.PartitionLockExpiresAt), Col(x => x.CreatedAt), Col(x => x.StartAfter), Col(x => x.EndBefore),
            Col(x => x.LastPlanCoverageUntil), Col(x => x.LastExecutedPlan), Col(x => x.HasFailedOnLastPlanExecution),
            Col(x => x.IsJobCancellationPending), Col(x => x.WorkerLane), Col(x => x.Version)
        };
        var vals = new[]
        {
            "@ClusterId","@Id","@Expression","@ExpressionTypeId",
            "@JobDefinitionId","@StaticDefinitionId","@ProfileId","@Status","@RecurringScheduleType",
            "@StaticDefinitionLastEnsured","@TerminatedAt","@MsgData","@MetadataJson","@Priority","@MaxNumberOfRetries",
            "@TimeoutTicks","@BucketId","@AgentConnectionId","@AgentWorkerId",
            "@PartitionLockId","@HostId","@HostDisplayName","@PartitionLockExpiresAt","@CreatedAt","@StartAfter","@EndBefore",
            "@LastPlanCoverageUntil","@LastExecutedPlan","@HasFailedOnLastPlanExecution",
            "@IsJobCancellationPending", "@WorkerLane", "@Version"
        };
        return (string.Join(", ", cols), string.Join(", ", vals));
    }

    protected string UpdateSetClause()
    {
        return string.Join(", ", new[]
        {
            $"{Col(x => x.Expression)} = @Expression",
            $"{Col(x => x.ExpressionTypeId)} = @ExpressionTypeId",
            $"{Col(x => x.JobDefinitionId)} = @JobDefinitionId",
            $"{Col(x => x.StaticDefinitionId)} = @StaticDefinitionId",
            $"{Col(x => x.ProfileId)} = @ProfileId",
            $"{Col(x => x.Status)} = @Status",
            $"{Col(x => x.RecurringScheduleType)} = @RecurringScheduleType",
            $"{Col(x => x.StaticDefinitionLastEnsured)} = @StaticDefinitionLastEnsured",
            $"{Col(x => x.TerminatedAt)} = @TerminatedAt",
            $"{Col(x => x.MsgData)} = @MsgData",
            $"{Col(x => x.Priority)} = @Priority",
            $"{Col(x => x.MaxNumberOfRetries)} = @MaxNumberOfRetries",
            $"{Col(x => x.TimeoutTicks)} = @TimeoutTicks",
            $"{Col(x => x.BucketId)} = @BucketId",
            $"{Col(x => x.AgentConnectionId)} = @AgentConnectionId",
            $"{Col(x => x.AgentWorkerId)} = @AgentWorkerId",
            $"{Col(x => x.PartitionLockId)} = @PartitionLockId",
            $"{Col(x => x.HostId)} = @HostId",
            $"{Col(x => x.HostDisplayName)} = @HostDisplayName",
            $"{Col(x => x.PartitionLockExpiresAt)} = @PartitionLockExpiresAt",
            $"{Col(x => x.StartAfter)} = @StartAfter",
            $"{Col(x => x.EndBefore)} = @EndBefore",
            $"{Col(x => x.LastPlanCoverageUntil)} = @LastPlanCoverageUntil",
            $"{Col(x => x.LastExecutedPlan)} = @LastExecutedPlan",
            $"{Col(x => x.HasFailedOnLastPlanExecution)} = @HasFailedOnLastPlanExecution",
            $"{Col(x => x.IsJobCancellationPending)} = @IsJobCancellationPending",
            $"{Col(x => x.WorkerLane)} = @WorkerLane",
            $"{Col(x => x.Version)} = @Version"
        });
    }
    
    protected (string sqlText, Dictionary<string, object?>) BuildGetByStaticIdSql(string staticId)
    {
        var t = TableName();
        var selectCols = SelectProjection();
        var sqlText = $@"
SELECT {selectCols}
FROM {t} s
WHERE s.{Col(x => x.StaticDefinitionId)} = @StaticDefinitionId
  AND s.{Col(x => x.ClusterId)} = @ClusterId
  AND s.{Col(x => x.RecurringScheduleType)} = @RecurringScheduleType";
        return (sqlText, new Dictionary<string, object?>
        {
            { "StaticDefinitionId", staticId },
            { "ClusterId", ClusterConnConfig.ClusterId },
            { "RecurringScheduleType", (int)RecurringScheduleType.Static }
        });
    }

    private string BuildUpdateSql()
    {
        var t = TableName();
        var cClusterId = Col(x => x.ClusterId);
        var cId = Col(x => x.Id);
        var cVersion = Col(x => x.Version);
        return $"UPDATE {t} SET {UpdateSetClause()} WHERE {cClusterId} = @ClusterId AND {cId} = @Id AND {cVersion} = @ExpectedVersion";
    }

    protected string Col(Expression<Func<SqlRecurringSchedulePersistenceRecordLinearDto, object?>> prop) => sql.ColumnNameFor(prop);

    // Renamed in spirit only -- was a GroupBy-based reconstruction of Metadata (rich object) from
    // flattened entry/value join rows, back when every read LEFT JOINed those tables. Reads no
    // longer join them (MetadataJson is read straight off the row), so this is now a plain 1:1
    // mapping; Metadata (rich object) is intentionally left unset here, same as Jobs' LinearListRecord.
    protected IList<SqlRecurringSchedulePersistenceRecord> LinearListToDomain(IList<SqlRecurringSchedulePersistenceRecordLinearDto> list)
    {
        return list.Select(row => new SqlRecurringSchedulePersistenceRecord
        {
            ClusterId = row.ClusterId,
            Id = row.Id,
            Expression = row.Expression,
            ExpressionTypeId = row.ExpressionTypeId,
            JobDefinitionId = row.JobDefinitionId,
            StaticDefinitionId = row.StaticDefinitionId,
            ProfileId = row.ProfileId,
            StaticDefinitionLastEnsured = row.StaticDefinitionLastEnsured,
            Status = row.Status,
            RecurringScheduleType = row.RecurringScheduleType,
            TerminatedAt = row.TerminatedAt,
            MsgData = row.MsgData,
            MetadataJson = row.MetadataJson,
            Priority = row.Priority,
            MaxNumberOfRetries = row.MaxNumberOfRetries,
            TimeoutTicks = row.TimeoutTicks,
            BucketId = row.BucketId,
            AgentConnectionId = row.AgentConnectionId,
            AgentWorkerId = row.AgentWorkerId,
            PartitionLockId = row.PartitionLockId,
            HostId = row.HostId,
            HostDisplayName = row.HostDisplayName,
            PartitionLockExpiresAt = row.PartitionLockExpiresAt,
            CreatedAt = row.CreatedAt,
            StartAfter = row.StartAfter,
            EndBefore = row.EndBefore,
            LastPlanCoverageUntil = row.LastPlanCoverageUntil,
            LastExecutedPlan = row.LastExecutedPlan,
            HasFailedOnLastPlanExecution = row.HasFailedOnLastPlanExecution,
            IsJobCancellationPending = row.IsJobCancellationPending,
            WorkerLane = row.WorkerLane,
            Version = row.Version,
        }).ToList<SqlRecurringSchedulePersistenceRecord>();
    }
    
    protected string UpdateSetClauseWithoutVersion()
    {
        return string.Join(", ", new[]
        {
            $"{Col(x => x.Expression)} = @Expression",
            $"{Col(x => x.ExpressionTypeId)} = @ExpressionTypeId",
            $"{Col(x => x.JobDefinitionId)} = @JobDefinitionId",
            $"{Col(x => x.StaticDefinitionId)} = @StaticDefinitionId",
            $"{Col(x => x.ProfileId)} = @ProfileId",
            $"{Col(x => x.Status)} = @Status",
            $"{Col(x => x.RecurringScheduleType)} = @RecurringScheduleType",
            $"{Col(x => x.StaticDefinitionLastEnsured)} = @StaticDefinitionLastEnsured",
            $"{Col(x => x.TerminatedAt)} = @TerminatedAt",
            $"{Col(x => x.MsgData)} = @MsgData",
            $"{Col(x => x.Priority)} = @Priority",
            $"{Col(x => x.MaxNumberOfRetries)} = @MaxNumberOfRetries",
            $"{Col(x => x.TimeoutTicks)} = @TimeoutTicks",
            $"{Col(x => x.BucketId)} = @BucketId",
            $"{Col(x => x.AgentConnectionId)} = @AgentConnectionId",
            $"{Col(x => x.AgentWorkerId)} = @AgentWorkerId",
            $"{Col(x => x.PartitionLockId)} = @PartitionLockId",
            $"{Col(x => x.HostId)} = @HostId",
            $"{Col(x => x.HostDisplayName)} = @HostDisplayName",
            $"{Col(x => x.PartitionLockExpiresAt)} = @PartitionLockExpiresAt",
            $"{Col(x => x.StartAfter)} = @StartAfter",
            $"{Col(x => x.EndBefore)} = @EndBefore",
            $"{Col(x => x.LastPlanCoverageUntil)} = @LastPlanCoverageUntil",
            $"{Col(x => x.LastExecutedPlan)} = @LastExecutedPlan",
            $"{Col(x => x.HasFailedOnLastPlanExecution)} = @HasFailedOnLastPlanExecution",
            $"{Col(x => x.IsJobCancellationPending)} = @IsJobCancellationPending",
            $"{Col(x => x.WorkerLane)} = @WorkerLane"
        });
    }

    protected class SqlRecurringSchedulePersistenceRecordLinearDto : SqlRecurringSchedulePersistenceRecord
    {
        public string RecordUniqueId { get; set; } = string.Empty;
        public string GroupId { get; set; } = string.Empty;
        public string EntryId { get; set; } = string.Empty;
        public Guid? EntryIdGuid { get; set; }

        public string KeyName { get; set; } = string.Empty;
        public long? ValueInt64 { get; set; }
        public decimal? ValueDecimal { get; set; }
        public string? ValueText { get; set; }
        public bool? ValueBool { get; set; }
        public DateTime? ValueDateTime { get; set; }
        public Guid? ValueGuid { get; set; }
        public byte[]? ValueBinary { get; set; }
    }
}