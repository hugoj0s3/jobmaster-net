using System.Data;
using System.Linq.Expressions;
using System.Text;
using Dapper;
using JobMaster.Sdk.Utils;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils.Extensions;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.SqlBase.Master;

internal abstract class SqlMasterJobsRepository : JobMasterClusterAwareRepository, IMasterJobsRepository
{
    private IDbConnectionManager connManager = null!;
    private ISqlGenerator sql = null!;
    private string connString = string.Empty;
    private JobMasterConfigDictionary additionalConnConfig = null!;
    private GenericRecordSqlUtil genericUtil = null!;

    protected SqlMasterJobsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connManager) : base(clusterConnectionConfig)
    {
        this.connManager = connManager;
        sql = SqlGeneratorFactory.Get(this.MasterRepoTypeId);
        connString = clusterConnectionConfig.ConnectionString;
        additionalConnConfig = clusterConnectionConfig.AdditionalConnConfig;
        genericUtil = new GenericRecordSqlUtil(sql, additionalConnConfig, ClusterConnConfig.ClusterId);
    }

    // IMasterJobsRepository
    public void Add(JobRawModel jobRaw)
    {
        try
        {
            using var conn = connManager.Open(connString, additionalConnConfig);
            using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

            var t = TableName();
            var rec = JobRawModel.ToPersistence(jobRaw);
            
            // Generate initial version for new job
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
            jobRaw.SetVersion(rec.Version);
        }
        catch (Exception ex) when (IsDupeViolation(jobRaw.Id, ex))
        {
            throw new JobDuplicationException(jobRaw.Id, ex);
        }
    }

    public async Task AddAsync(JobRawModel jobRaw)
    {
        try
        {
            using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
            using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

            var t = TableName();
            var rec = JobRawModel.ToPersistence(jobRaw);
            
            // Generate initial version for new job
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
            jobRaw.SetVersion(rec.Version);
        }
        catch (Exception ex) when (IsDupeViolation(jobRaw.Id, ex))
        {
            throw new JobDuplicationException(jobRaw.Id, ex);
        }
    }

    public void Update(JobRawModel jobRaw)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

        var t = TableName();
        var rec = JobRawModel.ToPersistence(jobRaw);
        var expectedVersion = rec.Version;
        
        // Generate new version in C# so we can update the model
        rec.Version = Guid.NewGuid().ToString("N").ToLowerInvariant();
        
        var setClause = UpdateSetClause();
        var sqlText = $"UPDATE {t} SET {setClause} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id AND ({Col(x => x.Version)} = @ExpectedVersion OR (@ExpectedVersion IS NULL AND {Col(x => x.Version)} IS NULL));";

        var rowsAffected = conn.Execute(sqlText, new { rec.Version, rec.ClusterId, rec.Id, ExpectedVersion = expectedVersion, rec.JobDefinitionId,
            triggerSourceType = rec.TriggerSourceType, rec.BucketId, rec.AgentConnectionId, rec.AgentWorkerId, rec.Priority, rec.ScheduledAt, rec.MsgData, rec.Status, rec.NumberOfFailures, rec.TimeoutTicks, rec.MaxNumberOfRetries, rec.RecurringScheduleId, rec.PartitionLockId, rec.PartitionLockExpiresAt, rec.ProcessDeadline, rec.ProcessingStartedAt, rec.SucceedExecutedAt, rec.WorkerLane }, trans);
        
        if (rowsAffected == 0)
        {
            trans.Rollback();
            var idExists = conn.ExecuteScalar<bool>("SELECT 1 FROM " + TableName() + " WHERE " + Col(x => x.ClusterId) + " = @ClusterId AND " + Col(x => x.Id) + " = @Id", new { rec.ClusterId, rec.Id });
            if (!idExists)
            {
                throw new Exception("Job not found");
            }
            
            throw new JobMasterVersionConflictException(jobRaw.Id, "Job", expectedVersion);
        }
        
        // Update the in-memory model with the new version
        jobRaw.SetVersion(rec.Version);

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

    public async Task UpdateAsync(JobRawModel jobRaw)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

        var t = TableName();
        var rec = JobRawModel.ToPersistence(jobRaw);
        var expectedVersion = rec.Version;
        
        // Generate new version in C# so we can update the model
        rec.Version = Guid.NewGuid().ToString("N").ToLowerInvariant();
        
        var setClause = UpdateSetClause();
        var sqlText = $"UPDATE {t} SET {setClause} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id AND ({Col(x => x.Version)} = @ExpectedVersion OR (@ExpectedVersion IS NULL AND {Col(x => x.Version)} IS NULL));";
        
        var rowsAffected = await conn.ExecuteAsync(sqlText, new { rec.Version, rec.ClusterId, rec.Id, ExpectedVersion = expectedVersion, rec.JobDefinitionId,
            TriggerSourceType = rec.TriggerSourceType, rec.BucketId, rec.AgentConnectionId, rec.AgentWorkerId, rec.Priority, rec.ScheduledAt, rec.MsgData, rec.Status, rec.NumberOfFailures, rec.TimeoutTicks, rec.MaxNumberOfRetries, rec.RecurringScheduleId, rec.PartitionLockId, rec.PartitionLockExpiresAt, rec.ProcessDeadline, rec.ProcessingStartedAt, rec.SucceedExecutedAt, rec.WorkerLane }, trans);
        
        if (rowsAffected == 0)
        {
            trans.Rollback();
            var idExists = conn.ExecuteScalar<bool>("SELECT 1 FROM " + TableName() + " WHERE " + Col(x => x.ClusterId) + " = @ClusterId AND " + Col(x => x.Id) + " = @Id", new { rec.ClusterId, rec.Id });
            if (!idExists)
            {
                throw new Exception("Job not found");
            }
            
            throw new JobMasterVersionConflictException(jobRaw.Id, "Job", expectedVersion);
        }
        
        // Update the in-memory model with the new version
        jobRaw.SetVersion(rec.Version);

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

    public IList<JobRawModel> Query(JobQueryCriteria queryCriteria)
    {
        using var conn = connManager.Open(connString, additionalConnConfig, queryCriteria.ReadIsolationLevel);
        var (sqlText, args) = BuildQuerySql(queryCriteria);
        var linearRows = conn.Query<JobPersistenceRecordLinearDto>(sqlText, args).ToList();
        var rows = LinearListRecord(linearRows);
        return rows.Select(JobRawModel.RecoverFromDb).ToList();
    }

    public async Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig, queryCriteria.ReadIsolationLevel);
        var (sqlText, args) = BuildQuerySql(queryCriteria);
        var linearRows = (await conn.QueryAsync<JobPersistenceRecordLinearDto>(sqlText, args)).ToList();
        var rows = LinearListRecord(linearRows);
        return rows.Select(JobRawModel.RecoverFromDb).ToList();
    }

    public JobRawModel? Get(Guid jobId)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var (sqlText, args) = BuildGetSql(jobId);
        var linearRows = conn.Query<JobPersistenceRecordLinearDto>(sqlText, args).ToList();
        var rows = LinearListRecord(linearRows);

        return rows.Select(JobRawModel.RecoverFromDb).SingleOrDefault();
    }

    public async Task<JobRawModel?> GetAsync(Guid jobId)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var (sqlText, args) = BuildGetSql(jobId);

        var linearRows = (await conn.QueryAsync<JobPersistenceRecordLinearDto>(sqlText, args)).ToList();
        var rows = LinearListRecord(linearRows);

        return rows.Select(JobRawModel.RecoverFromDb).SingleOrDefault();
    }

    public long Count(JobQueryCriteria queryCriteria)
    {
        using var conn = connManager.Open(connString, additionalConnConfig, ReadIsolationLevel.FastSync);
        var (whereSql, args) = BuildWhere(queryCriteria);
        args.Add("GroupId", MasterGenericRecordGroupIds.JobMetadata);
        var t = TableName();
        var sqlText = @$"
SELECT COUNT(*) 
FROM {t} j 
LEFT JOIN {genericUtil.EntryTable()} e ON e.{Col(x => x.EntryIdGuid)} = j.{Col(x => x.Id)} and e.{Col(x => x.GroupId)} = @GroupId
{whereSql}";
        return conn.ExecuteScalar<long>(sqlText, args);
    }

    public IList<Guid> QueryIds(JobQueryCriteria queryCriteria)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var (whereSql, args) = BuildWhere(queryCriteria);
        var sb = new StringBuilder();
        sb.Append($"SELECT {Col(x => x.Id)} FROM {TableName()} j {whereSql}");
        sb.Append($" ORDER BY j.{Col(x => x.ScheduledAt)} ASC, j.{Col(x => x.CreatedAt)} ASC");
        if (queryCriteria.CountLimit > 0)
        {
            sb.Append('\n');
            sb.Append(sql.OffsetQueryFor(queryCriteria.CountLimit, queryCriteria.Offset));
        }
        return conn.Query<Guid>(sb.ToString(), args).ToList();
    }

    public async Task<IList<Guid>> QueryIdsAsync(JobQueryCriteria queryCriteria)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var (whereSql, args) = BuildWhere(queryCriteria);
        var sb = new StringBuilder();
        sb.Append($"SELECT {Col(x => x.Id)} FROM {TableName()} j {whereSql}");
        sb.Append($" ORDER BY j.{Col(x => x.ScheduledAt)} ASC, j.{Col(x => x.CreatedAt)} ASC");
        if (queryCriteria.CountLimit > 0)
        {
            sb.Append('\n');
            sb.Append(sql.OffsetQueryFor(queryCriteria.CountLimit, queryCriteria.Offset));
        }
        return (await conn.QueryAsync<Guid>(sb.ToString(), args)).ToList();
    }

    public bool BulkUpdatePartitionLockId(IList<Guid> jobIds, int lockId, DateTime expiresAt)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var distinctJobIds = jobIds.Distinct().ToList();
            var t = TableName();

            var sqlText = @$"
            UPDATE {t} SET 
                {Col(x => x.PartitionLockId)} = @LockId, 
                {Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
                {Col(x => x.Version)} = {sql.GenerateVersionSql()}
            WHERE {this.sql.InClauseFor(Col(x => x.Id), "@JobIds")} 
              AND ({Col(x => x.PartitionLockId)} IS NULL OR {Col(x => x.PartitionLockExpiresAt)} < @NowUtc)";

            var rowsAffected = conn.Execute(sqlText, new
            {
                JobIds = distinctJobIds,
                LockId = lockId,
                LockExpiresAt = expiresAt,
                NowUtc = JobMasterConstants.NowUtcWithSkewTolerance()
            }, trans);

            if (rowsAffected != distinctJobIds.Count)
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

    public void ClearPartitionLock(Guid jobId)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var t = TableName();

        var sqlText = @$"
        UPDATE {t} SET 
            {Col(x => x.PartitionLockId)} = NULL, 
            {Col(x => x.PartitionLockExpiresAt)} = NULL,
            {Col(x => x.Version)} = {sql.GenerateVersionSql()}
        WHERE {Col(x => x.Id)} = @JobId";

        conn.Execute(sqlText, new { JobId = jobId });
    }

    public void BulkUpdateStatus(IList<Guid> jobIds, JobMasterJobStatus status, string? agentConnectionId, string? agentWorkerId, string? bucketId, IList<JobMasterJobStatus>? excludeStatuses = null)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var inIds = this.sql.InClauseFor(Col(x => x.Id), "@JobIds");
            var where = inIds;
            var hasExclude = excludeStatuses != null && excludeStatuses.Count > 0;
            if (hasExclude)
            {
                var notInStatuses = this.sql.InClauseFor(Col(x => x.Status), "@NegateStatuses");
                where += $" AND NOT ({notInStatuses})";
            }

            var sqlText =
                $"UPDATE {t} SET {Col(x => x.Status)} = @Status, {Col(x => x.AgentConnectionId)} = @AgentConnectionId, {Col(x => x.AgentWorkerId)} = @AgentWorkerId, {Col(x => x.BucketId)} = @BucketId, {Col(x => x.Version)} = {sql.GenerateVersionSql()} WHERE {where}";
            var args = new
            {
                JobIds = jobIds,
                Status = status,
                AgentConnectionId = agentConnectionId,
                AgentWorkerId = agentWorkerId,
                BucketId = bucketId,
                NegateStatuses = hasExclude ? excludeStatuses!.Select(s => (int)s).ToList() : null
            };
            conn.Execute(sqlText, args, trans);
            trans.Commit();
        }
        catch (Exception)
        {
            trans.Rollback();
            throw;
        }
    }

    public async Task<int> PurgeFinalByScheduledAtAsync(DateTime cutoffUtc, int limit)
    {
        if (limit <= 0) throw new ArgumentException("limit must be > 0", nameof(limit));

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            // 1) Select candidate IDs limited for portability
            var t = TableName();
            var cId = Col(x => x.Id);
            var cClusterId = Col(x => x.ClusterId);
            var cStatus = Col(x => x.Status);
            var cScheduledAt = Col(x => x.ScheduledAt);

            var selectSql = new StringBuilder($@"SELECT {cId} FROM {t}
WHERE {cClusterId} = @ClusterId
  AND {cStatus} IN (@Succeeded, @Failed, @Cancelled)
  AND {cScheduledAt} <= @CutoffUtc
ORDER BY {cScheduledAt} ASC, {cId} ASC");
            selectSql.Append('\n');
            selectSql.Append(sql.OffsetQueryFor(limit, 0));

            var ids = (await conn.QueryAsync<Guid>(selectSql.ToString(), new
            {
                ClusterId = ClusterConnConfig.ClusterId,
                CutoffUtc = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Utc),
                Succeeded = (int)JobMasterJobStatus.Succeeded,
                Failed = (int)JobMasterJobStatus.Failed,
                Cancelled = (int)JobMasterJobStatus.Cancelled,
            }, tx)).ToList();

            if (ids.Count == 0)
            {
                tx.Commit();
                return 0;
            }

            // 2) Delete by IDs
            var affected = 0;
            foreach (var idsPartition in ids.Partition(JobMasterConstants.MaxBatchSizeForBulkOperation).ToList())
            {
                // Delete Jobs
                var inClause = sql.InClauseFor(cId, "@Ids");
                var deleteSql = $"DELETE FROM {t} WHERE {cClusterId} = @ClusterId AND {inClause}";
                affected += await conn.ExecuteAsync(deleteSql, new { ClusterId = ClusterConnConfig.ClusterId, Ids = idsPartition }, tx);

                // Delete Metadata associated
                var metadataUniqueIds = idsPartition.Select(id => GenericRecordEntry.UniqueId(this.ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.JobMetadata, id)).ToList();
                var deleteMetadataValuesSql = genericUtil.BuildDeleteValuesMultipleSql("@metadataUniqueIds");

                await conn.ExecuteAsync(deleteMetadataValuesSql, new { ClusterId = ClusterConnConfig.ClusterId, metadataUniqueIds }, tx);

                var deleteMetadataEntrySql = genericUtil.BuildDeleteEntryMultipleSql("@metadataUniqueIds");
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
    
    protected abstract bool IsDupeViolation(Guid jobId, Exception ex);

    // SQL builders
    private (string, object) BuildGetSql(Guid jobId)
    {
        var selectCols = SelectProjection();
        // var sqlText = $"SELECT {selectCols} FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id";
        var sqlText = $@"
SELECT {selectCols} 
FROM {TableName()} j
LEFT JOIN {genericUtil.EntryTable()} e ON e.{Col(x => x.EntryIdGuid)} = j.{Col(x => x.Id)} and e.{Col(x => x.GroupId)} = @GroupId
LEFT JOIN {genericUtil.EntryValueTable()} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)} 
 WHERE j.{Col(x => x.ClusterId)} = @ClusterId AND j.{Col(x => x.Id)} = @Id
";
        var args = new { ClusterId = ClusterConnConfig.ClusterId, Id = jobId, GroupId = MasterGenericRecordGroupIds.JobMetadata };
        return (sqlText, args);
    }

    private (string, object) BuildQuerySql(JobQueryCriteria c)
    {
        var t = TableName();
        var selectCols = SelectProjection();
        var order = "j.scheduled_at ASC, j.created_at ASC";
        var (whereSql, args) = BuildWhere(c);
        var (queryText, queryArgs) = BuildQuery(t, selectCols, whereSql, order);

        var concatedArgs = args.Concat(queryArgs).ToDictionary(x => x.Key, x => x.Value);
        
        var sb = new StringBuilder();
        sb.Append(queryText);
        if (c.CountLimit > 0)
        {
            sb.Append('\n');
            sb.Append(sql.OffsetQueryFor(c.CountLimit, c.Offset));
        }

        return (sb.ToString(), concatedArgs);
    }

    private (string whereSql, Dictionary<string, object?> args) BuildWhere(JobQueryCriteria c)
    {
        var where = new List<string> { $"j.{Col(x => x.ClusterId)} = @ClusterId" };
        var args = new Dictionary<string, object?>();
        args.Add("ClusterId", ClusterConnConfig.ClusterId);

        if (c.Status.HasValue)
        {
            where.Add($"j.{Col(x => x.Status)} = @Status");
            args.Add("Status", (int)c.Status.Value);
        }

        if (c.ScheduledFrom.HasValue)
        {
            where.Add($"j.{Col(x => x.ScheduledAt)} >= @ScheduledFrom");
            args.Add("ScheduledFrom", c.ScheduledFrom.Value);
        }

        if (c.ScheduledTo.HasValue)
        {
            where.Add($"j.{Col(x => x.ScheduledAt)} <= @ScheduledTo");
            args.Add("ScheduledTo", c.ScheduledTo.Value);
        }

        if (c.ProcessDeadlineTo.HasValue)
        {
            where.Add($"j.{Col(x => x.ProcessDeadline)} <= @ProcessDeadlineTo");
            args.Add("ProcessDeadlineTo", c.ProcessDeadlineTo.Value);
        }

        if (c.IsLocked.HasValue)
        {
            where.Add(c.IsLocked.Value
                ? $"(j.{Col(x => x.PartitionLockId)} IS NOT NULL AND j.{Col(x => x.PartitionLockExpiresAt)} > @NowUtc)"
                : $"(j.{Col(x => x.PartitionLockId)} IS NULL OR j.{Col(x => x.PartitionLockExpiresAt)} < @NowUtcWithSkewPadding)");
            args.Add("NowUtc", DateTime.UtcNow);
            args.Add("NowUtcWithSkewPadding", JobMasterConstants.NowUtcWithSkewTolerance());
        }

        if (c.PartitionLockId.HasValue)
        {
            where.Add($"j.{Col(x => x.PartitionLockId)} = @PartitionLockId");
            args.Add("PartitionLockId", c.PartitionLockId.Value);
        }

        if (c.RecurringScheduleId.HasValue)
        {
            where.Add($"j.{Col(x => x.RecurringScheduleId)} = @RecurringScheduleId");
            args.Add("RecurringScheduleId", c.RecurringScheduleId.Value);
        }

        if (!string.IsNullOrEmpty(c.JobDefinitionId))
        {
            where.Add($"j.{Col(x => x.JobDefinitionId)} = @JobDefinitionId");
            args.Add("JobDefinitionId", c.JobDefinitionId);
        }

        if (!string.IsNullOrEmpty(c.WorkerLane))
        {
            where.Add($"j.{Col(x => x.WorkerLane)} = @WorkerLane");
            args.Add("WorkerLane", c.WorkerLane);
        }

        var exists = genericUtil.BuildWhereClause(c.MetadataFilters, "e", "existsV", args);
        if (!string.IsNullOrEmpty(exists)) where.Add(exists);

        var whereSql = "WHERE " + string.Join(" AND ", where);
        return (whereSql, args);
    }

    private string TableName()
    {
        return sql.TableNameFor<Job>(additionalConnConfig);
    }

    private (string Columns, string ValuesParams) InsertColumnsAndParams()
    {
        var cols = new[]
        {
            Col(x => x.ClusterId), Col(x => x.Id), Col(x => x.JobDefinitionId), Col(x => x.TriggerSourceType),
            Col(x => x.BucketId), Col(x => x.AgentConnectionId), Col(x => x.AgentWorkerId), Col(x => x.Priority),
            Col(x => x.OriginalScheduledAt), Col(x => x.ScheduledAt), Col(x => x.MsgData), Col(x => x.Status),
            Col(x => x.NumberOfFailures), Col(x => x.TimeoutTicks), Col(x => x.MaxNumberOfRetries),
            Col(x => x.CreatedAt), Col(x => x.RecurringScheduleId),
            Col(x => x.PartitionLockId), Col(x => x.PartitionLockExpiresAt), Col(x => x.ProcessDeadline),
            Col(x => x.ProcessingStartedAt), Col(x => x.SucceedExecutedAt),
            Col(x => x.WorkerLane), Col(x => x.Version)
        };
        var vals = new[]
        {
            "@ClusterId", "@Id", "@JobDefinitionId", "@TriggerSourceType",
            "@BucketId", "@AgentConnectionId", "@AgentWorkerId", "@Priority",
            "@OriginalScheduledAt", "@ScheduledAt", "@MsgData", "@Status",
            "@NumberOfFailures", "@TimeoutTicks", "@MaxNumberOfRetries",
            "@CreatedAt", "@RecurringScheduleId",
            "@PartitionLockId", "@PartitionLockExpiresAt", "@ProcessDeadline",
            "@ProcessingStartedAt", "@SucceedExecutedAt",
            "@WorkerLane", "@Version",
        };
        return (string.Join(", ", cols), string.Join(", ", vals));
    }

    private string UpdateSetClause()
    {
        // All mutable fields except ClusterId/Id/OriginalScheduledAt/CreatedAt
        return string.Join(", ", new[]
        {
            $"{Col(x => x.JobDefinitionId)} = @JobDefinitionId",
            $"{Col(x => x.TriggerSourceType)} = @TriggerSourceType",
            $"{Col(x => x.BucketId)} = @BucketId",
            $"{Col(x => x.AgentConnectionId)} = @AgentConnectionId",
            $"{Col(x => x.AgentWorkerId)} = @AgentWorkerId",
            $"{Col(x => x.Priority)} = @Priority",
            $"{Col(x => x.ScheduledAt)} = @ScheduledAt",
            $"{Col(x => x.MsgData)} = @MsgData",
            $"{Col(x => x.Status)} = @Status",
            $"{Col(x => x.NumberOfFailures)} = @NumberOfFailures",
            $"{Col(x => x.TimeoutTicks)} = @TimeoutTicks",
            $"{Col(x => x.MaxNumberOfRetries)} = @MaxNumberOfRetries",
            $"{Col(x => x.RecurringScheduleId)} = @RecurringScheduleId",
            $"{Col(x => x.PartitionLockId)} = @PartitionLockId",
            $"{Col(x => x.PartitionLockExpiresAt)} = @PartitionLockExpiresAt",
            $"{Col(x => x.ProcessDeadline)} = @ProcessDeadline",
            $"{Col(x => x.ProcessingStartedAt)} = @ProcessingStartedAt",
            $"{Col(x => x.SucceedExecutedAt)} = @SucceedExecutedAt",
            $"{Col(x => x.WorkerLane)} = @WorkerLane",
            $"{Col(x => x.Version)} = @Version",
        });
    }

    private string SelectProjection(string jobAlias = "j", string genericEntryAlias = "e", string genericEntryValueAlias = "v")
    {
        // No aliases needed; Dapper will map snake_case -> PascalCase
        return string.Join(", ", new[]
        {
            $"{jobAlias}.{Col(x => x.ClusterId)}",
            $"{jobAlias}.{Col(x => x.Id)}",
            $"{jobAlias}.{Col(x => x.JobDefinitionId)}",
            $"{jobAlias}.{Col(x => x.TriggerSourceType)}",
            $"{jobAlias}.{Col(x => x.BucketId)}",
            $"{jobAlias}.{Col(x => x.AgentConnectionId)}",
            $"{jobAlias}.{Col(x => x.AgentWorkerId)}",
            $"{jobAlias}.{Col(x => x.Priority)}",
            $"{jobAlias}.{Col(x => x.OriginalScheduledAt)}",
            $"{jobAlias}.{Col(x => x.ScheduledAt)}",
            $"{jobAlias}.{Col(x => x.MsgData)}",
            $"{jobAlias}.{Col(x => x.Status)}",
            $"{jobAlias}.{Col(x => x.NumberOfFailures)}",
            $"{jobAlias}.{Col(x => x.TimeoutTicks)}",
            $"{jobAlias}.{Col(x => x.MaxNumberOfRetries)}",
            $"{jobAlias}.{Col(x => x.CreatedAt)}",
            $"{jobAlias}.{Col(x => x.RecurringScheduleId)}",
            $"{jobAlias}.{Col(x => x.PartitionLockId)}",
            $"{jobAlias}.{Col(x => x.PartitionLockExpiresAt)}",
            $"{jobAlias}.{Col(x => x.ProcessDeadline)}",
            $"{jobAlias}.{Col(x => x.ProcessingStartedAt)}",
            $"{jobAlias}.{Col(x => x.SucceedExecutedAt)}",
            $"{jobAlias}.{Col(x => x.WorkerLane)}",
            $"{jobAlias}.{Col(x => x.Version)}",

            // Entry
            $"{genericEntryAlias}.{Col(x => x.RecordUniqueId)}",
            $"{genericEntryAlias}.{Col(x => x.GroupId)}",
            $"{genericEntryAlias}.{Col(x => x.EntryId)}",

            // EntryValue
            $"{genericEntryValueAlias}.{Col(x => x.KeyName)}",
            $"{genericEntryValueAlias}.{Col(x => x.ValueInt64)}",
            $"{genericEntryValueAlias}.{Col(x => x.ValueDecimal)}",
            $"{genericEntryValueAlias}.{Col(x => x.ValueText)}",
            $"{genericEntryValueAlias}.{Col(x => x.ValueBool)}",
            $"{genericEntryValueAlias}.{Col(x => x.ValueDateTime)}",
            $"{genericEntryValueAlias}.{Col(x => x.ValueGuid)}"
        });
    }

    private (string sqlText, IDictionary<string, object?> args) BuildQuery(string jobTableName, string selectCols, string whereSql, string order)
    {
        var sqlText =
            $@"
SELECT {selectCols} 
FROM {jobTableName} j
LEFT JOIN {genericUtil.EntryTable()} e ON e.{Col(x => x.EntryIdGuid)} = j.{Col(x => x.Id)} and e.{Col(x => x.GroupId)} = @GroupId
LEFT JOIN {genericUtil.EntryValueTable()} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)} 
{whereSql}
ORDER BY {order}";
        
        return (sqlText, new Dictionary<string, object?> { { "GroupId", MasterGenericRecordGroupIds.JobMetadata } });
    }

    private string Col(Expression<Func<JobPersistenceRecordLinearDto, object?>> prop) => sql.ColumnNameFor(prop);

    private IList<JobPersistenceRecord> LinearListRecord(IList<JobPersistenceRecordLinearDto> list)
    {
        if (list.Count == 0) return new List<JobPersistenceRecord>(0);

        var result = new List<JobPersistenceRecord>();
        foreach (var jobGroup in list.GroupBy(x => x.Id))
        {
            var first = jobGroup.First();

            // Build metadata dictionary from linear rows (skip when no key)
            var kvs = new Dictionary<string, object?>(StringComparer.Ordinal);
            string? groupId = null;
            string? entryId = null;
            foreach (var row in jobGroup)
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
            // Since GenericRecordEntry.FromWritableMetadata expects IWritableMetadata,
            // build via Contracts.Models.Metadata which implements IWritable/IReadable.
            if (kvs.Count > 0 && !string.IsNullOrEmpty(groupId) && !string.IsNullOrEmpty(entryId))
            {
                var metaWritable = WritableMetadata.FromDictionary(kvs);
                metadata = GenericRecordEntry.FromWritableMetadata(
                    ClusterConnConfig.ClusterId,
                    groupId!,
                    entryId!,
                    metaWritable
                );
            }

            var rec = new JobPersistenceRecord
            {
                ClusterId = first.ClusterId,
                Id = first.Id,
                JobDefinitionId = first.JobDefinitionId,
                TriggerSourceType = first.TriggerSourceType,
                BucketId = first.BucketId,
                AgentConnectionId = first.AgentConnectionId,
                AgentWorkerId = first.AgentWorkerId,
                Priority = first.Priority,
                OriginalScheduledAt = first.OriginalScheduledAt,
                ScheduledAt = first.ScheduledAt,
                MsgData = first.MsgData,
                Status = first.Status,
                NumberOfFailures = first.NumberOfFailures,
                TimeoutTicks = first.TimeoutTicks,
                MaxNumberOfRetries = first.MaxNumberOfRetries,
                CreatedAt = first.CreatedAt,
                RecurringScheduleId = first.RecurringScheduleId,
                PartitionLockId = first.PartitionLockId,
                PartitionLockExpiresAt = first.PartitionLockExpiresAt,
                ProcessDeadline = first.ProcessDeadline,
                ProcessingStartedAt = first.ProcessingStartedAt,
                SucceedExecutedAt = first.SucceedExecutedAt,
                Metadata = metadata,
                WorkerLane = first.WorkerLane,
                Version = first.Version,
            };

            result.Add(rec);
        }

        return result;
    }

    private class JobPersistenceRecordLinearDto : JobPersistenceRecord
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