using System.Data;
using System.Linq.Expressions;
using System.Text;
using Dapper;
using JobMaster.Contracts.Extensions;
using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Exceptions;
using JobMaster.Sdk.Contracts.Jobs;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sql.Connections;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sql.Scripts;

namespace JobMaster.Sql.Master;

public abstract class SqlMasterRecurringSchedulesRepository : JobMasterClusterAwareRepository, IMasterRecurringSchedulesRepository
{
    private IDbConnectionManager connManager = null!;
    private ISqlGenerator sql = null!;
    private string connString = string.Empty;
    private JobMasterConfigDictionary additionalConnConfig = null!;
    private GenericRecordSqlUtil genericUtil = null!;

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

    public void Add(RecurringScheduleRawModel scheduleRaw)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        var t = TableName();
        var rec = RecurringScheduleRawModel.ToPersistence(scheduleRaw);
        
        // Generate initial version for new recurring schedule
        rec.Version = Guid.NewGuid().ToString("N").ToLowerInvariant();
        
        var (cols, vals) = InsertColumnsAndParams();
        var sqlText = $"INSERT INTO {t} ({cols}) VALUES ({vals});";
        conn.Execute(sqlText, rec, trans);

        if (rec.Metadata is not null)
        {
            var sqlEntry = genericUtil.MapToSqlEntry(rec.Metadata);
            var (insertSql, parameters) = genericUtil.BuildInsertEntrySql(sqlEntry);
            conn.Execute(insertSql, parameters, trans);

            var (insertValuesSql, paramRows) = genericUtil.BuildInsertEntryValuesSql(sqlEntry);
            conn.Execute(insertValuesSql, paramRows, trans);
        }
        
        trans.Commit();
        
        // Update the in-memory model with the new version
        scheduleRaw.Version = rec.Version;
    }

    public async Task AddAsync(RecurringScheduleRawModel scheduleRaw)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        var t = TableName();
        var rec = RecurringScheduleRawModel.ToPersistence(scheduleRaw);
        
        // Generate initial version for new recurring schedule
        rec.Version = Guid.NewGuid().ToString("N").ToLowerInvariant();
        
        var (cols, vals) = InsertColumnsAndParams();
        var sqlText = $"INSERT INTO {t} ({cols}) VALUES ({vals});";
        await conn.ExecuteAsync(sqlText, rec, trans);

        if (rec.Metadata is not null)
        {
            var sqlEntry = genericUtil.MapToSqlEntry(rec.Metadata);
            var (insertSql, parameters) = genericUtil.BuildInsertEntrySql(sqlEntry);
            await conn.ExecuteAsync(insertSql, parameters, trans);

            var (insertValuesSql, paramRows) = genericUtil.BuildInsertEntryValuesSql(sqlEntry);
            await conn.ExecuteAsync(insertValuesSql, paramRows, trans);
        }
        
        trans.Commit();
        
        // Update the in-memory model with the new version
        scheduleRaw.Version = rec.Version;
    }

    public void Update(RecurringScheduleRawModel scheduleRaw)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        var t = TableName();
        var rec = RecurringScheduleRawModel.ToPersistence(scheduleRaw);
        var expectedVersion = rec.Version;
        
        // Generate new version
        rec.Version = Guid.NewGuid().ToString("N").ToLowerInvariant();
        
        var setClause = UpdateSetClause();
        var sqlText = $"UPDATE {t} SET {setClause} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id AND ({Col(x => x.Version)} = @ExpectedVersion OR (@ExpectedVersion IS NULL AND {Col(x => x.Version)} IS NULL));";
        
        var rowsAffected = conn.Execute(sqlText, new { rec.Version, rec.ClusterId, rec.Id, ExpectedVersion = expectedVersion, rec.Expression, rec.ExpressionTypeId, rec.JobDefinitionId, rec.StaticDefinitionId, rec.ProfileId, rec.Status, rec.RecurringScheduleType, rec.StaticDefinitionLastEnsured,
            rec.TerminatedAt, rec.MsgData, rec.Priority, rec.MaxNumberOfRetries, rec.TimeoutTicks, rec.BucketId, rec.AgentConnectionId, rec.AgentWorkerId, rec.PartitionLockId, rec.PartitionLockExpiresAt, rec.CreatedAt, rec.StartAfter, rec.EndBefore, rec.LastPlanCoverageUntil, rec.LastExecutedPlan, rec.HasFailedOnLastPlanExecution, rec.IsJobCancellationPending, rec.WorkerLane }, trans);
        
        if (rowsAffected == 0)
        {
            trans.Rollback();
            var idExists = conn.ExecuteScalar<bool>("SELECT 1 FROM " + TableName() + " WHERE " + Col(x => x.ClusterId) + " = @ClusterId AND " + Col(x => x.Id) + " = @Id", new { rec.ClusterId, rec.Id });
            if (!idExists)
            {
                throw new Exception("Recurring Schedule not found");
            }
            
            throw new JobMasterVersionConflictException(scheduleRaw.Id, "RecurringSchedule", expectedVersion);
        }
        
        // Update the in-memory model with the new version
        scheduleRaw.Version = rec.Version;

        if (rec.Metadata is not null)
        {
            var sqlEntry = genericUtil.MapToSqlEntry(rec.Metadata);
            var (updateSql, parameters) = genericUtil.BuildUpdateEntrySql(sqlEntry);
            conn.Execute(updateSql, parameters, trans);

            var deleteValueSql = genericUtil.BuildDeleteValuesSql();
            conn.Execute(deleteValueSql, new { RecordUniqueId = sqlEntry.RecordUniqueId }, trans);

            var (insertValuesSql, paramRows) = genericUtil.BuildInsertEntryValuesSql(sqlEntry);
            conn.Execute(insertValuesSql, paramRows, trans);
        }
        
        trans.Commit();
    }

    public async Task UpdateAsync(RecurringScheduleRawModel scheduleRaw)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        var t = TableName();
        var rec = RecurringScheduleRawModel.ToPersistence(scheduleRaw);
        var expectedVersion = rec.Version;
        
        // Generate new version
        rec.Version = Guid.NewGuid().ToString("N").ToLowerInvariant();
        
        var setClause = UpdateSetClause();
        var sqlText = $"UPDATE {t} SET {setClause} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id AND ({Col(x => x.Version)} = @ExpectedVersion OR (@ExpectedVersion IS NULL AND {Col(x => x.Version)} IS NULL));";
        
        var rowsAffected = await conn.ExecuteAsync(sqlText, new { rec.Version, rec.ClusterId, rec.Id, ExpectedVersion = expectedVersion, rec.Expression, rec.ExpressionTypeId, rec.JobDefinitionId, rec.StaticDefinitionId, rec.ProfileId, rec.Status, rec.RecurringScheduleType, rec.StaticDefinitionLastEnsured,
            rec.TerminatedAt, rec.MsgData, rec.Priority, rec.MaxNumberOfRetries, rec.TimeoutTicks, rec.BucketId, rec.AgentConnectionId, rec.AgentWorkerId, rec.PartitionLockId, rec.PartitionLockExpiresAt, rec.CreatedAt, rec.StartAfter, rec.EndBefore, rec.LastPlanCoverageUntil, rec.LastExecutedPlan, rec.HasFailedOnLastPlanExecution, rec.IsJobCancellationPending, rec.WorkerLane }, trans);
        
        if (rowsAffected == 0)
        {
            trans.Rollback();
            var idExists = conn.ExecuteScalar<bool>("SELECT 1 FROM " + TableName() + " WHERE " + Col(x => x.ClusterId) + " = @ClusterId AND " + Col(x => x.Id) + " = @Id", new { rec.ClusterId, rec.Id });
            if (!idExists)
            {
                throw new Exception("Recurring Schedule not found");
            }
            
            throw new JobMasterVersionConflictException(scheduleRaw.Id, "RecurringSchedule", expectedVersion);
        }
        
        // Update the in-memory model with the new version
        scheduleRaw.Version = rec.Version;

        if (rec.Metadata is not null)
        {
            var sqlEntry = genericUtil.MapToSqlEntry(rec.Metadata);
            var (updateSql, parameters) = genericUtil.BuildUpdateEntrySql(sqlEntry);
            await conn.ExecuteAsync(updateSql, parameters, trans);

            var deleteValueSql = genericUtil.BuildDeleteValuesSql();
            await conn.ExecuteAsync(deleteValueSql, new { RecordUniqueId = sqlEntry.RecordUniqueId }, trans);

            var (insertValuesSql, paramRows) = genericUtil.BuildInsertEntryValuesSql(sqlEntry);
            await conn.ExecuteAsync(insertValuesSql, paramRows, trans);
        }
        
        trans.Commit();
    }

    public IList<RecurringScheduleRawModel> Query(RecurringScheduleQueryCriteria queryCriteria)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var (sqlText, args) = BuildQuerySql(queryCriteria);
        var linearRows = conn.Query<RecurringSchedulePersistenceRecordLinearDto>(sqlText, args).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(RecurringScheduleRawModel.RecoverFromDb).ToList();
    }

    public async Task<IList<RecurringScheduleRawModel>> QueryAsync(RecurringScheduleQueryCriteria queryCriteria)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var (sqlText, args) = BuildQuerySql(queryCriteria);
        var linearRows = (await conn.QueryAsync<RecurringSchedulePersistenceRecordLinearDto>(sqlText, args)).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(RecurringScheduleRawModel.RecoverFromDb).ToList();
    }

    public RecurringScheduleRawModel? Get(Guid recurringScheduleId)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var (sqlText, args) = BuildGetSql(recurringScheduleId);
        var linearRows = conn.Query<RecurringSchedulePersistenceRecordLinearDto>(sqlText, args).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(RecurringScheduleRawModel.RecoverFromDb).SingleOrDefault();
    }

    public async Task<RecurringScheduleRawModel?> GetAsync(Guid recurringScheduleId)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var (sqlText, args) = BuildGetSql(recurringScheduleId);
        var linearRows = (await conn.QueryAsync<RecurringSchedulePersistenceRecordLinearDto>(sqlText, args)).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(RecurringScheduleRawModel.RecoverFromDb).SingleOrDefault();
    }

    public RecurringScheduleRawModel? GetByStaticId(string staticId)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var (sqlText, args) = BuildGetByStaticIdSql(staticId);
        var linearRows = conn.Query<RecurringSchedulePersistenceRecordLinearDto>(sqlText, args).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(RecurringScheduleRawModel.RecoverFromDb).SingleOrDefault();
    }

    public bool BulkUpdatePartitionLockId(IList<Guid> recurringScheduleIds, int lockId, DateTime expiresAt)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var sqlText = @$"
UPDATE {t} SET 
    {Col(x => x.PartitionLockId)} = @LockId, 
    {Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    {Col(x => x.Version)} = {sql.GenerateVersionSql()}
WHERE {this.sql.InClauseFor(Col(x => x.Id), "@RecurringScheduleIds")} 
    AND ({Col(x => x.PartitionLockId)} is null OR {Col(x => x.PartitionLockExpiresAt)} < @NowUtc) ";
            
            var rowsAffected = conn.Execute(sqlText, new { RecurringScheduleIds = recurringScheduleIds, LockId = lockId, LockExpiresAt = expiresAt, NowUtc = JobMasterConstants.NowUtcWithSkewTolerance() }, trans);
            
            // lock all or nothing.
            if (rowsAffected != recurringScheduleIds.Count)
            {
                trans.Rollback();
                return false;
            }
            
            trans.Commit();
            
            return true;
        }
        catch (Exception)
        {
            trans.Rollback();
            throw;
        }
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
            tx.Rollback();
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
            trans.Rollback();
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

    public async Task<int> PurgeTerminatedAsync(DateTime cutoffUtc, int limit)
    {
        if (limit <= 0) throw new ArgumentException("limit must be > 0", nameof(limit));

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var cId = Col(x => x.Id);
            var cClusterId = Col(x => x.ClusterId);
            var cStatus = Col(x => x.Status);
            var cTerminatedAt = Col(x => x.TerminatedAt);

            var selectSql = new StringBuilder($@"SELECT {cId} FROM {t}
WHERE {cClusterId} = @ClusterId
  AND {cStatus} IN (@Inactive, @Canceled, @Completed)
  AND {cTerminatedAt} <= @CutoffUtc
ORDER BY {cTerminatedAt} ASC, {cId} ASC");
            selectSql.Append('\n');
            selectSql.Append(sql.OffsetQueryFor(limit, 0));

            var ids = (await conn.QueryAsync<Guid>(selectSql.ToString(), new
            {
                ClusterId = ClusterConnConfig.ClusterId,
                CutoffUtc = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Utc),
                Inactive = (int)RecurringScheduleStatus.Inactive,
                Canceled = (int)RecurringScheduleStatus.Canceled,
                Completed = (int)RecurringScheduleStatus.Completed
            }, tx)).ToList();

            if (ids.Count == 0)
            {
                tx.Commit();
                return 0;
            }
            
            var affected = 0;
            foreach (var idsPartition in ids.Partition(JobMasterConstants.MaxBatchSizeForBulkOperation).ToList())
            {
                var inClause = sql.InClauseFor(cId, "@Ids");
                var deleteSql = $"DELETE FROM {t} WHERE {cClusterId} = @ClusterId AND {inClause}";
                affected += await conn.ExecuteAsync(deleteSql, new { ClusterId = ClusterConnConfig.ClusterId, Ids = idsPartition }, tx);

                // Delete associated metadata
                var metadataUniqueIds = idsPartition.Select(id => GenericRecordEntry.UniqueId(this.ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.RecurringScheduleMetadata, id)).ToList();
                var deleteMetadataValuesSql = genericUtil.BuildDeleteValuesMultipleSql("metadataUniqueIds");
                await conn.ExecuteAsync(deleteMetadataValuesSql, new { ClusterId = ClusterConnConfig.ClusterId, metadataUniqueIds }, tx);

                var deleteMetadataEntrySql = genericUtil.BuildDeleteEntryMultipleSql("metadataUniqueIds");
                await conn.ExecuteAsync(deleteMetadataEntrySql, new { ClusterId = ClusterConnConfig.ClusterId, metadataUniqueIds }, tx);
            }

            tx.Commit();
            return affected;
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }

    // SQL builders
    private (string, object) BuildGetSql(Guid id)
    {
        var t = TableName();
        var selectCols = SelectProjection("s", "e", "v");
        var sqlText = $@"
SELECT {selectCols} 
FROM {t} s 
LEFT JOIN {genericUtil.EntryTable()} e ON e.{Col(x => x.EntryIdGuid)} = s.{Col(x => x.Id)} 
LEFT JOIN {genericUtil.EntryValueTable()} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)} 
WHERE s.{Col(x => x.ClusterId)} = @ClusterId AND s.{Col(x => x.Id)} = @Id";
        var args = new { ClusterId = ClusterConnConfig.ClusterId, Id = id };
        return (sqlText, args);
    }

    private (string, object) BuildQuerySql(RecurringScheduleQueryCriteria c)
    {
        var t = TableName();
        var selectCols = SelectProjection("s", "e", "v");
        var (whereSql, args) = BuildWhere(c);
        var order = $" s.{Col(x => x.LastPlanCoverageUntil)} DESC, s.{Col(x => x.CreatedAt)} ASC";
        var sb = new StringBuilder();
        sb.Append($@"
SELECT {selectCols} FROM {t} s
LEFT JOIN {genericUtil.EntryTable()} e ON e.{Col(x => x.EntryIdGuid)} = s.{Col(x => x.Id)} 
LEFT JOIN {genericUtil.EntryValueTable()} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)}  
{whereSql} 
ORDER BY {order}");
        if (c.CountLimit > 0)
        {
            sb.Append('\n');
            sb.Append(sql.OffsetQueryFor(c.CountLimit, c.Offset));
        }
        return (sb.ToString(), args);
    }

    private (string, Dictionary<string, object?>) BuildWhere(RecurringScheduleQueryCriteria c)
    {
        var where = new List<string> { $"s.{Col(x => x.ClusterId)} = @ClusterId" };
        var args = new Dictionary<string, object?>();
        args.Add("ClusterId", ClusterConnConfig.ClusterId);

        if (c.Status.HasValue)
        {
            where.Add($"s.{Col(x => x.Status)} = @Status");
            args.Add("Status", (int)c.Status.Value);
        }
        
        if (c.IsLocked.HasValue)
        {
            where.Add(c.IsLocked.Value ? 
                $"(s.{Col(x => x.PartitionLockId)} IS NOT NULL AND s.{Col(x => x.PartitionLockExpiresAt)} > @NowUtc)" : 
                $"(s.{Col(x => x.PartitionLockId)} IS NULL OR s.{Col(x => x.PartitionLockExpiresAt)} < @NowUtcWithSkewPadding)");
            args.Add("NowUtc", DateTime.UtcNow);
            args.Add("NowUtcWithSkewPadding", JobMasterConstants.NowUtcWithSkewTolerance());
        }
        
        if (c.PartitionLockId.HasValue)
        {
            where.Add($"s.{Col(x => x.PartitionLockId)} = @PartitionLockId");
            args.Add("PartitionLockId", c.PartitionLockId.Value);
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
        
        if (c.RecurringScheduleType.HasValue)
        {
            where.Add($"s.{Col(x => x.RecurringScheduleType)} = @RecurringScheduleType");
            args.Add("RecurringScheduleType", (int)c.RecurringScheduleType.Value);
        }
        
        var metadataFilter = genericUtil.BuildWhereClause(c.MetadataFilters, "e", "existsV", args);
        if (!string.IsNullOrEmpty(metadataFilter))
        {
            where.Add(metadataFilter);
        }

        var whereSql = "WHERE " + string.Join(" AND ", where);
        return (whereSql, args);
    }

    private string TableName()
    {
        return sql.TableNameFor<RecurringSchedule>(additionalConnConfig);
    }

    private string SelectProjection(string scheduleAlias = "s", string genericEntryAlias = "e", string genericEntryValueAlias = "v")
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
            $"{scheduleAlias}.{Col(x => x.Priority)}",
            $"{scheduleAlias}.{Col(x => x.MaxNumberOfRetries)}",
            $"{scheduleAlias}.{Col(x => x.TimeoutTicks)}",
            $"{scheduleAlias}.{Col(x => x.BucketId)}",
            $"{scheduleAlias}.{Col(x => x.AgentConnectionId)}",
            $"{scheduleAlias}.{Col(x => x.AgentWorkerId)}",
            $"{scheduleAlias}.{Col(x => x.PartitionLockId)}",
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

            // Generic entry
            $"{genericEntryAlias}.{Col(x => x.RecordUniqueId)}",
            $"{genericEntryAlias}.{Col(x => x.GroupId)}",
            $"{genericEntryAlias}.{Col(x => x.EntryId)}",
            $"{genericEntryAlias}.{Col(x => x.EntryIdGuid)}",

            // Entry values
            $"{genericEntryValueAlias}.{Col(x => x.KeyName)}",
            $"{genericEntryValueAlias}.{Col(x => x.ValueInt64)}",
            $"{genericEntryValueAlias}.{Col(x => x.ValueDecimal)}",
            $"{genericEntryValueAlias}.{Col(x => x.ValueText)}",
            $"{genericEntryValueAlias}.{Col(x => x.ValueBool)}",
            $"{genericEntryValueAlias}.{Col(x => x.ValueDateTime)}",
            $"{genericEntryValueAlias}.{Col(x => x.ValueGuid)}"
        });
    }

    private (string Columns, string ValuesParams) InsertColumnsAndParams()
    {
        var cols = new[]
        {
            Col(x => x.ClusterId), Col(x => x.Id), Col(x => x.Expression), Col(x => x.ExpressionTypeId),
            Col(x => x.JobDefinitionId), Col(x => x.StaticDefinitionId), Col(x => x.ProfileId), Col(x => x.Status), Col(x => x.RecurringScheduleType),
            Col(x => x.StaticDefinitionLastEnsured), Col(x => x.TerminatedAt), Col(x => x.MsgData), Col(x => x.Priority), Col(x => x.MaxNumberOfRetries),
            Col(x => x.TimeoutTicks), Col(x => x.BucketId), Col(x => x.AgentConnectionId), Col(x => x.AgentWorkerId),
            Col(x => x.PartitionLockId), Col(x => x.PartitionLockExpiresAt), Col(x => x.CreatedAt), Col(x => x.StartAfter), Col(x => x.EndBefore),
            Col(x => x.LastPlanCoverageUntil), Col(x => x.LastExecutedPlan), Col(x => x.HasFailedOnLastPlanExecution),
            Col(x => x.IsJobCancellationPending), Col(x => x.WorkerLane), Col(x => x.Version)
        };
        var vals = new[]
        {
            "@ClusterId","@Id","@Expression","@ExpressionTypeId",
            "@JobDefinitionId","@StaticDefinitionId","@ProfileId","@Status","@RecurringScheduleType",
            "@StaticDefinitionLastEnsured","@TerminatedAt","@MsgData","@Priority","@MaxNumberOfRetries",
            "@TimeoutTicks","@BucketId","@AgentConnectionId","@AgentWorkerId",
            "@PartitionLockId","@PartitionLockExpiresAt","@CreatedAt","@StartAfter","@EndBefore",
            "@LastPlanCoverageUntil","@LastExecutedPlan","@HasFailedOnLastPlanExecution",
            "@IsJobCancellationPending", "@WorkerLane", "@Version"
        };
        return (string.Join(", ", cols), string.Join(", ", vals));
    }

    private string UpdateSetClause()
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
    
    private (string sqlText, Dictionary<string, object?> args) BuildGetByStaticIdSql(string staticId)
    {
        var t = TableName();
        var sqlText = $@"
SELECT * 
FROM {t} s 
LEFT JOIN {genericUtil.EntryTable()} e ON e.{Col(x => x.EntryIdGuid)} = s.{Col(x => x.Id)} 
LEFT JOIN {genericUtil.EntryValueTable()} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)} 
WHERE s.{Col(x => x.StaticDefinitionId)} = @StaticDefinitionId 
  and s.{Col(x => x.ClusterId)} = @ClusterId
  and s.{Col(x => x.RecurringScheduleType)} = @RecurringScheduleType";
        return (sqlText, new Dictionary<string, object?>
        {
            { "StaticDefinitionId", staticId },
            { "ClusterId", ClusterConnConfig.ClusterId },
            { "RecurringScheduleType", (int) RecurringScheduleType.Static }
        });
    }

    private string Col(Expression<Func<RecurringSchedulePersistenceRecordLinearDto, object?>> prop) => sql.ColumnNameFor(prop);

    private IList<RecurringSchedulePersistenceRecord> LinearListToDomain(IList<RecurringSchedulePersistenceRecordLinearDto> list)
    {
        if (list.Count == 0) return new List<RecurringSchedulePersistenceRecord>(0);

        var result = new List<RecurringSchedulePersistenceRecord>();
        foreach (var group in list.GroupBy(x => x.Id))
        {
            var first = group.First();

            var kvs = new Dictionary<string, object?>(StringComparer.Ordinal);
            string? entryId = null;
            string? groupId = null;
            foreach (var row in group)
            {
                if (string.IsNullOrEmpty(row.KeyName)) continue;
                groupId ??= row.GroupId;
                entryId ??= row.EntryId;

                object? val = row.ValueText ?? 
                              (object?)row.ValueBinary ?? 
                              row.ValueInt64 ?? 
                              row.ValueBool ?? 
                              (object?)row.ValueDecimal ?? 
                              (object?)row.ValueDateTime ?? 
                              row.ValueGuid;
                kvs[row.KeyName] = val;
            }

            GenericRecordEntry? metadata = null;
            if (kvs.Count > 0 && !string.IsNullOrEmpty(groupId) && !string.IsNullOrEmpty(entryId))
            {
                var metaWritable = new Metadata(kvs);
                metadata = GenericRecordEntry.FromWritableMetadata(ClusterConnConfig.ClusterId, groupId!, entryId!, metaWritable);
            }

            var rec = new RecurringSchedulePersistenceRecord
            {
                ClusterId = first.ClusterId,
                Id = first.Id,
                Expression = first.Expression,
                ExpressionTypeId = first.ExpressionTypeId,
                JobDefinitionId = first.JobDefinitionId,
                StaticDefinitionId = first.StaticDefinitionId,
                ProfileId = first.ProfileId,
                StaticDefinitionLastEnsured = first.StaticDefinitionLastEnsured,
                Status = first.Status,
                RecurringScheduleType = first.RecurringScheduleType,
                TerminatedAt = first.TerminatedAt,
                MsgData = first.MsgData,
                Priority = first.Priority,
                MaxNumberOfRetries = first.MaxNumberOfRetries,
                TimeoutTicks = first.TimeoutTicks,
                BucketId = first.BucketId,
                AgentConnectionId = first.AgentConnectionId,
                AgentWorkerId = first.AgentWorkerId,
                PartitionLockId = first.PartitionLockId,
                PartitionLockExpiresAt = first.PartitionLockExpiresAt,
                CreatedAt = first.CreatedAt,
                StartAfter = first.StartAfter,
                EndBefore = first.EndBefore,
                LastPlanCoverageUntil = first.LastPlanCoverageUntil,
                LastExecutedPlan = first.LastExecutedPlan,
                HasFailedOnLastPlanExecution = first.HasFailedOnLastPlanExecution,
                IsJobCancellationPending = first.IsJobCancellationPending,
                Metadata = metadata,
                WorkerLane = first.WorkerLane,
                Version = first.Version,
            };

            result.Add(rec);
        }

        return result;
    }

    private class RecurringSchedulePersistenceRecordLinearDto : RecurringSchedulePersistenceRecord
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