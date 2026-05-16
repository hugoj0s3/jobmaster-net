using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
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
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.SqlBase.Master;

internal abstract class SqlMasterJobsRepository : JobMasterClusterAwareRepository, IMasterJobsRepository
{
    protected IDbConnectionManager connManager = null!;
    protected ISqlGenerator sql = null!;
    protected string connString = string.Empty;
    protected JobMasterConfigDictionary additionalConnConfig = null!;
    protected GenericRecordSqlUtil genericUtil = null!;
    protected readonly IKnownExceptionIdentifier knownExceptionIdentifier;

    protected SqlMasterJobsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connManager,
        IKnownExceptionIdentifier knownExceptionIdentifier) : base(clusterConnectionConfig)
    {
        this.connManager = connManager;
        this.knownExceptionIdentifier = knownExceptionIdentifier;
        sql = SqlGeneratorFactory.Get(this.MasterRepoTypeId);
        connString = clusterConnectionConfig.ConnectionString;
        additionalConnConfig = clusterConnectionConfig.AdditionalConnConfig;
        genericUtil = new GenericRecordSqlUtil(sql, additionalConnConfig, ClusterConnConfig.ClusterId);
    }

    // IMasterJobsRepository
    public void Add(JobRawModel jobRaw)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var rec = JobRawModel.ToPersistence(jobRaw);
            if (rec.Metadata is not null)
            {
                var sqlEntry = genericUtil.MapToSqlEntry(rec.Metadata);
                var (insertSql, parameters) = genericUtil.BuildInsertEntrySql(sqlEntry);
                conn.Execute(insertSql, parameters, trans);

                var (insertValuesSql, paramRows) = genericUtil.BuildInsertEntryValuesSql(sqlEntry);
                conn.Execute(insertValuesSql, paramRows, trans);
            }
            
            // Generate initial version for new job
            rec.Version = JobMasterRandomUtil.NewGuid4().ToString("N").ToLowerInvariant();
            
            var (cols, vals) = InsertColumnsAndParams();
            var sqlText = $"INSERT INTO {t} ({cols}) VALUES ({vals});";
            conn.Execute(sqlText, rec, trans);

            trans.Commit();
            
            // Update the in-memory model with the new version
            jobRaw.SetVersion(rec.Version);
        }
        catch (Exception ex) when (IsDupeViolation(jobRaw.Id, ex))
        {
            trans.SafeRollback();
            throw new JobMasterDuplicationException(jobRaw.Id, "Job", ex);
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    public async Task AddAsync(JobRawModel jobRaw)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var rec = JobRawModel.ToPersistence(jobRaw);
            if (rec.Metadata is not null)
            {
                var sqlEntry = genericUtil.MapToSqlEntry(rec.Metadata);
                var (insertSql, parameters) = genericUtil.BuildInsertEntrySql(sqlEntry);
                await conn.ExecuteAsync(insertSql, parameters, trans);

                var (insertValuesSql, paramRows) = genericUtil.BuildInsertEntryValuesSql(sqlEntry);
                await conn.ExecuteAsync(insertValuesSql, paramRows, trans);
            }
            
            // Generate initial version for new job
            rec.Version = JobMasterRandomUtil.NewGuid4().ToString("N").ToLowerInvariant();
            
            var (cols, vals) = InsertColumnsAndParams();
            var sqlText = $"INSERT INTO {t} ({cols}) VALUES ({vals});";
            await conn.ExecuteAsync(sqlText, rec, trans);
            

            trans.Commit();
            
            // Update the in-memory model with the new version
            jobRaw.SetVersion(rec.Version);
        }
        catch (Exception ex) when (IsDupeViolation(jobRaw.Id, ex))
        {
            trans.SafeRollback();
            throw new JobMasterDuplicationException(jobRaw.Id, "Job", ex);
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    public void Update(JobRawModel jobRaw, JobExecution? addJobExecution = null)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var rec = JobRawModel.ToPersistence(jobRaw);
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
                    throw new JobMasterVersionConflictException(jobRaw.Id, "Job", expectedVersion);
            }

            if (addJobExecution != null)
            {
                conn.Execute(BuildJobExecutionInsertSql(), BuildJobExecutionParams(addJobExecution), trans);
            }

            trans.Commit();
            jobRaw.SetVersion(rec.Version);
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    public async Task UpdateAsync(JobRawModel jobRaw, JobExecution? addJobExecution = null)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var rec = JobRawModel.ToPersistence(jobRaw);
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
                    throw new JobMasterVersionConflictException(jobRaw.Id, "Job", expectedVersion);
            }

            if (addJobExecution != null)
            {
                await conn.ExecuteAsync(BuildJobExecutionInsertSql(), BuildJobExecutionParams(addJobExecution), trans);
            }

            trans.Commit();
            jobRaw.SetVersion(rec.Version);
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    public async Task AddJobExecutionAsync(JobExecution jobExecution)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            await conn.ExecuteAsync(BuildJobExecutionInsertSql(), BuildJobExecutionParams(jobExecution), trans);
            trans.Commit();
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    public async Task<IList<JobExecution>> QueryJobExecutionsAsync(Guid jobId)
    {
        var t = JobExecutionTableName();
        var sqlText = $@"
SELECT cluster_id, id, job_id, started_at, agent_connection_id, agent_worker_id, bucket_id, host_id, host_display_name, finalized_at, outcome_message, outcome
FROM {t}
WHERE cluster_id = @ClusterId AND job_id = @JobId
ORDER BY started_at DESC";

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var rows = (await conn.QueryAsync<JobExecutionPersistenceRecord>(sqlText,
            new { ClusterId = ClusterConnConfig.ClusterId, JobId = jobId })).ToList();

        return rows.Select(JobExecution.RecoverFromDb).ToList();
    }

    public bool Exists(Guid jobId)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var sqlText = $"SELECT 1 FROM {TableName()} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id";
        return conn.ExecuteScalar<bool>(sqlText, new { ClusterId = ClusterConnConfig.ClusterId, Id = jobId });
    }

    public async Task<bool> ExistsAsync(Guid jobId)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var sqlText = $"SELECT 1 FROM {TableName()} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id";
        return await conn.ExecuteScalarAsync<bool>(sqlText, new { ClusterId = ClusterConnConfig.ClusterId, Id = jobId });
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
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = j.{Col(x => x.Id)} and e.{Col(x => x.GroupId)} = @GroupId
{whereSql}";
        return conn.ExecuteScalar<long>(sqlText, args);
    }

    public void ReleasePartitionLock(Guid jobId)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var t = TableName();

        var sqlText = @$"
        UPDATE {t} SET 
            {Col(x => x.PartitionLockId)} = NULL, 
            {Col(x => x.PartitionLockExpiresAt)} = NULL,
            {Col(x => x.Version)} = {sql.GenerateVersionSql()}
        WHERE {Col(x => x.ClusterId)} = @ClusterId and  
            {Col(x => x.Id)} = @JobId";

        conn.Execute(sqlText, new { ClusterId = this.ClusterConnConfig.ClusterId, JobId = jobId });
    }

    public async Task BulkUpdateAsync(BulkJobUpdateRequest request)
    {
        if (request.JobIds.Count == 0 || request.Properties.Count == 0) return;

        var t = TableName();
        var args = new DynamicParameters();
        args.Add("ClusterId", ClusterConnConfig.ClusterId);
        args.Add("JobIds", request.JobIds);

        var setClauses = new List<string>(request.Properties.Count + 1);
        for (var i = 0; i < request.Properties.Count; i++)
        {
            var field = request.Properties[i];
            var colName = sql.ColumnNameFor(field.Expression);
            var paramName = $"f{i}";
            setClauses.Add($"{colName} = @{paramName}");
            args.Add(paramName, field.Value);
        }
        setClauses.Add($"{Col(x => x.Version)} = {sql.GenerateVersionSql()}");

        var inIds = sql.InClauseFor(Col(x => x.Id), "@JobIds");
        var whereSql = $"{Col(x => x.ClusterId)} = @ClusterId AND {inIds}";

        if (request.ExcludeStatuses is { Count: > 0 })
        {
            args.Add("ExcludeStatuses", request.ExcludeStatuses.Select(x => (int)x).ToList());
            var notIn = sql.InClauseFor(Col(x => x.Status), "@ExcludeStatuses");
            whereSql += $" AND NOT ({notIn})";
        }

        var sqlText = $"UPDATE {t} SET {string.Join(", ", setClauses)} WHERE {whereSql}";

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            await conn.ExecuteAsync(sqlText, args, trans);
            trans.Commit();
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    public async Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs)
    {
        if (jobs.Count == 0) return Array.Empty<JobRawModel>();

        var t = TableName();
        var cId = Col(x => x.Id);
        var cClusterId = Col(x => x.ClusterId);
        var setClause = $"{UpdateSetClauseWithoutVersion()}, {Col(x => x.Version)} = @Version";
        var sqlText = $"UPDATE {t} SET {setClause} WHERE {cClusterId} = @ClusterId AND {cId} = @Id";

        var newVersions = new Dictionary<Guid, string>(jobs.Count);
        foreach (var job in jobs)
        {
            newVersions[job.Id] = JobMasterRandomUtil.NewGuid4().ToString("N").ToLowerInvariant();
        }

        var updated = new List<JobRawModel>(jobs.Count);

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            foreach (var job in jobs)
            {
                var rec = JobRawModel.ToPersistence(job);
                rec.Version = newVersions[job.Id];
                var rowsAffected = await conn.ExecuteAsync(sqlText, rec, trans);
                if (rowsAffected > 0)
                {
                    updated.Add(job);
                }
            }
            trans.Commit();
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }

        foreach (var job in updated)
        {
            job.SetVersion(newVersions[job.Id]);
        }

        return updated;
    }

    public async Task<int> PurgeFinalizedAsync(DateTime cutoffUtc, int limit)
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
            var cFinalizedAt = Col(x => x.FinalizedAt);

            var selectSql = new StringBuilder($@"SELECT {cId} FROM {t}
WHERE {cClusterId} = @ClusterId
  AND {cStatus} IN (@Succeeded, @Failed, @Cancelled, @Aborted)
  AND {cFinalizedAt} IS NOT NULL
  AND {cFinalizedAt} <= @CutoffUtc
ORDER BY {cFinalizedAt} ASC, {cId} ASC");
            selectSql.Append('\n');
            selectSql.Append(sql.OffsetQueryFor(limit, 0));

            var ids = (await conn.QueryAsync<Guid>(selectSql.ToString(), new
            {
                ClusterId = ClusterConnConfig.ClusterId,
                CutoffUtc = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Utc),
                Succeeded = (int)JobMasterJobStatus.Succeeded,
                Failed = (int)JobMasterJobStatus.Failed,
                Cancelled = (int)JobMasterJobStatus.Cancelled,
                Aborted = (int)JobMasterJobStatus.Aborted,
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
                var deleteMetadataValuesSql = genericUtil.BuildDeleteValuesMultipleSql(MasterGenericRecordGroupIds.JobMetadata, "@metadataUniqueIds");

                await conn.ExecuteAsync(deleteMetadataValuesSql, new { ClusterId = ClusterConnConfig.ClusterId, metadataUniqueIds }, tx);

                var deleteMetadataEntrySql = genericUtil.BuildDeleteEntryMultipleSql(MasterGenericRecordGroupIds.JobMetadata, "@metadataUniqueIds");
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

    public virtual async Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, Guid partitionLockId,
        DateTime expiresAtUtc)
    {
        if (partitionLockId == Guid.Empty) throw new ArgumentException("partitionLockId must be a valid GUID", nameof(partitionLockId));
        if (queryCriteria == null) throw new ArgumentNullException(nameof(queryCriteria));

        var nowUtcWithSkew = JobMasterConstants.NowUtcWithSkewTolerance();
        var expiresAtUtcKind = DateTime.SpecifyKind(expiresAtUtc, DateTimeKind.Utc);
        
        var unlockedGuard = $"({Col(x => x.PartitionLockId)} IS NULL OR {Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig, ReadIsolationLevel.Consistent);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var (whereSql, args) = BuildWhere(queryCriteria);
            var needsMetadataJoin = queryCriteria.MetadataFilters is { Count: > 0 };
            var queryIdsSql = 
                BuildQueryIdsToLockSql(whereSql, needsMetadataJoin, queryCriteria.CountLimit, queryCriteria.Offset, queryCriteria.SortBy);
            var updateSql = $@"
UPDATE {t} {UpdateToLockTableHint}
SET {Col(x => x.PartitionLockId)} = @PartitionLockId,
    {Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    {Col(x => x.Version)} = {sql.GenerateVersionSql()}
where {Col(x => x.Id)} in ({queryIdsSql})
  AND {unlockedGuard}
  ;";
            var args2 = new Dictionary<string, object?>(args);
            args2.Add("LockNowUtc", nowUtcWithSkew);
            args2.Add("LockExpiresAt", expiresAtUtcKind);
            args2.Add("PartitionLockId", partitionLockId);

            var rowsAffected = await conn.ExecuteAsync(updateSql, args2, trans);

            trans.Commit();
            
            if (rowsAffected == 0) return new List<JobRawModel>();

            using var conn2 = await connManager.OpenAsync(connString, additionalConnConfig, ReadIsolationLevel.Consistent);
            return await QueryLockedJobsAsync(partitionLockId, nowUtcWithSkew, conn2);
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    private async Task<IList<JobRawModel>> QueryLockedJobsAsync(
        Guid partitionLockId,
        DateTime nowUtcWithSkew,
        IDbConnection cnn2)
    {
        var selectCols = SelectProjection();
        var t = TableName();
        var cClusterId = Col(x => x.ClusterId);
        var cPartitionLockId = Col(x => x.PartitionLockId);
        var cPartitionLockExpiresAt = Col(x => x.PartitionLockExpiresAt);

        var sqlText = $@"
SELECT {selectCols} 
FROM {t} j
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata)} e 
    ON e.{Col(x => x.EntryIdGuid)} = j.{Col(x => x.Id)} 
    AND e.{Col(x => x.GroupId)} = @GroupId
LEFT JOIN {genericUtil.EntryValueTable(MasterGenericRecordGroupIds.JobMetadata)} v 
    ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)} 
WHERE j.{cClusterId} = @ClusterId
  AND j.{cPartitionLockId} = @PartitionLockId 
  AND j.{cPartitionLockExpiresAt} > @NowUtcWithSkew";

        var args = new Dictionary<string, object?>
        {
            { "GroupId", MasterGenericRecordGroupIds.JobMetadata },
            { "ClusterId", ClusterConnConfig.ClusterId },
            { "PartitionLockId", partitionLockId },
            { "NowUtcWithSkew", nowUtcWithSkew }
        };

        var linearRows = (await cnn2.QueryAsync<JobPersistenceRecordLinearDto>(sqlText, args)).ToList();
        var rows = LinearListRecord(linearRows);
        return rows.Select(JobRawModel.RecoverFromDb).ToList();
    }
    
    protected virtual string UpdateToLockTableHint => string.Empty;
    
    protected virtual bool IsDupeViolation(Guid jobId, Exception ex)
    {
        return knownExceptionIdentifier.Identify(ex) == JobMasterKnownExceptionId.DuplicateKey;
    }

    // SQL builders
    private (string, object) BuildGetSql(Guid jobId)
    {
        var selectCols = SelectProjection();
        // var sqlText = $"SELECT {selectCols} FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id";
        var sqlText = $@"
SELECT {selectCols} 
FROM {TableName()} j
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = j.{Col(x => x.Id)} and e.{Col(x => x.GroupId)} = @GroupId
LEFT JOIN {genericUtil.EntryValueTable(MasterGenericRecordGroupIds.JobMetadata)} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)} 
 WHERE j.{Col(x => x.ClusterId)} = @ClusterId AND j.{Col(x => x.Id)} = @Id
";
        var args = new { ClusterId = ClusterConnConfig.ClusterId, Id = jobId, GroupId = MasterGenericRecordGroupIds.JobMetadata };
        return (sqlText, args);
    }
    
    protected virtual string BuildQueryIdsToLockSql( 
        string whereSql, 
        bool needsMetadataJoin,
        int countLimit,
        int offset,
        SortByCriteria? sortByCriteria
        )
    {
        var metadataJoin = string.Empty;
        if (needsMetadataJoin)
        {
            metadataJoin =
                @$"
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata)} e ON 
    e.{Col(x => x.EntryIdGuid)} = j.{Col(x => x.Id)} and 
    e.{Col(x => x.GroupId)} = @GroupId ";
        }
            
        var sb = new StringBuilder();
        var sqlText = $@"
SELECT {Col(x => x.Id)} 
FROM {TableName()} j
{metadataJoin}
{whereSql}";
        sb.Append(sqlText);
        var sortBy = SqlOrderByUtil.BuildOrderByClause(sortByCriteria, "j", $" ORDER BY j.{Col(x => x.NextPlanExecutionAt)} ASC");
        
        sb.Append(sortBy);
        
        sb.Append('\n');
        sb.Append(sql.OffsetQueryFor(countLimit, offset));
        return sb.ToString();
    }


    private (string, object) BuildQuerySql(JobQueryCriteria c, int? partitionLockId = null, bool? isLocked = false)
    {
        var t = TableName();
        var selectCols = SelectProjection();
        var defaultOrderByClause =  $" ORDER BY j.{Col(x => x.NextPlanExecutionAt)} ASC";
        var order = SqlOrderByUtil.BuildOrderByClause(c.SortBy, "j", defaultOrderByClause);
        
        
        if (c.CountLimit < 0) 
            throw new ArgumentOutOfRangeException(nameof(c.CountLimit), c.CountLimit, "CountLimit must be >= 0");
        if (c.Offset < 0) 
            throw new ArgumentOutOfRangeException(nameof(c.Offset), c.Offset, "Offset must be >= 0");
        
        var (whereSql, args) = BuildWhere(c, partitionLockId, isLocked);

        var concatedArgs = args.Concat(new Dictionary<string, object?> { { "GroupId", MasterGenericRecordGroupIds.JobMetadata } })
            .ToDictionary(x => x.Key, x => x.Value);

        if (c.CountLimit > 0)
        {
            var offsetClause = sql.OffsetQueryFor(c.CountLimit, c.Offset);

            var queryText = $@"
WITH jobs_page AS (
    SELECT j.*
    FROM {t} j
    LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = j.{Col(x => x.Id)} and e.{Col(x => x.GroupId)} = @GroupId
    {whereSql}
    {order}
    {offsetClause}
)
SELECT {selectCols}
FROM jobs_page j
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = j.{Col(x => x.Id)} and e.{Col(x => x.GroupId)} = @GroupId
LEFT JOIN {genericUtil.EntryValueTable(MasterGenericRecordGroupIds.JobMetadata)} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)}
{order}";

            return (queryText, concatedArgs);
        }

        var (queryTextNoPaging, queryArgsNoPaging) = BuildQuery(t, selectCols, whereSql, order);
        var concatedArgsNoPaging = concatedArgs.Concat(queryArgsNoPaging).ToDictionary(x => x.Key, x => x.Value);
        return (queryTextNoPaging, concatedArgsNoPaging);
    }

    protected (string whereSql, Dictionary<string, object?> args) BuildWhere(
        JobQueryCriteria c, 
        int? partitionLockId = null, 
        bool? isLocked = false)
    {
        var where = new List<string> { $"j.{Col(x => x.ClusterId)} = @ClusterId" };
        var args = new Dictionary<string, object?>();
        args.Add("ClusterId", ClusterConnConfig.ClusterId);

        if (c.JobIds is { Count: > 0 })
        {
            var inClause = sql.InClauseFor(Col(x => x.Id), "@JobIds");
            where.Add(inClause);
            args.Add("JobIds", c.JobIds.ToArray());
        }

        if (c.ExcludeJobIds is { Count: > 0 })
        {
            var notInClause = sql.InClauseFor($"j.{Col(x => x.Id)}", "@ExcludeJobIds");
            where.Add($"NOT ({notInClause})");
            args.Add("ExcludeJobIds", c.ExcludeJobIds.ToArray());
        }

        if (c.Status.HasValue)
        {
            where.Add($"j.{Col(x => x.Status)} = @Status");
            args.Add("Status", (int)c.Status.Value);
        }

        if (!c.Statuses.IsNullOrEmpty())
        {
            where.Add($"j.{Col(x => x.Status)} IN ({string.Join(", ", c.Statuses.Select(s => $"@Status_{s}"))})");
            foreach (var status in c.Statuses)
            {
                args.Add($"Status_{status}", (int)status);
            }
        }

        if (c.NextPlanExecutionAtFrom.HasValue)
        {
            where.Add($"j.{Col(x => x.NextPlanExecutionAt)} >= @NextPlanExecutionAtFrom");
            args.Add("NextPlanExecutionAtFrom", c.NextPlanExecutionAtFrom.Value);
        }

        if (c.NextPlanExecutionAtTo.HasValue)
        {
            where.Add($"j.{Col(x => x.NextPlanExecutionAt)} <= @NextPlanExecutionAtTo");
            args.Add("NextPlanExecutionAtTo", c.NextPlanExecutionAtTo.Value);
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

        if (isLocked.HasValue)
        {
            where.Add(isLocked.Value
                ? $"(j.{Col(x => x.PartitionLockId)} IS NOT NULL AND j.{Col(x => x.PartitionLockExpiresAt)} > @NowUtc)"
                : $"(j.{Col(x => x.PartitionLockId)} IS NULL OR j.{Col(x => x.PartitionLockExpiresAt)} < @NowUtcWithSkewPadding)");
            args.Add("NowUtc", DateTime.UtcNow);
            args.Add("NowUtcWithSkewPadding", JobMasterConstants.NowUtcWithSkewTolerance());
        }

        if (partitionLockId.HasValue)
        {
            where.Add($"j.{Col(x => x.PartitionLockId)} = @PartitionLockId");
            args.Add("PartitionLockId", partitionLockId.Value);
        }

        if (c.TriggerSourceTypes is { Count: > 0 })
        {
            var inClause = sql.InClauseFor($"j.{Col(x => x.TriggerSourceType)}", "@TriggerSourceTypes");
            where.Add(inClause);
            args.Add("TriggerSourceTypes", c.TriggerSourceTypes.Select(x => (int)x).ToArray());
        }

        if (c.SourceId.HasValue)
        {
            where.Add($"j.{Col(x => x.SourceId)} = @SourceId");
            args.Add("SourceId", c.SourceId.Value);
        }
        
        if (c.SourceIds is {Count: > 0})
        {
            var inClause = sql.InClauseFor($"j.{Col(x => x.SourceId)}", "@SourceIds");
            where.Add(inClause);
            args.Add("SourceIds", c.SourceIds.ToArray());
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

        if (c.ExcludeBucketIds is { Count: > 0 })
        {
            var notInClause = sql.InClauseFor($"j.{Col(x => x.BucketId)}", "@ExcludeBucketIds");
            where.Add($"NOT ({notInClause})");
            args.Add("ExcludeBucketIds", c.ExcludeBucketIds.ToArray());
        }

        var exists = genericUtil.BuildWhereClause(c.MetadataFilters, "e", "existsV", args, MasterGenericRecordGroupIds.JobMetadata);
        if (!string.IsNullOrEmpty(exists)) where.Add(exists);

        var whereSql = "WHERE " + string.Join(" AND ", where);
        return (whereSql, args);
    }

    protected string TableName()
    {
        return sql.TableNameFor<Job>(additionalConnConfig);
    }

    private string JobExecutionTableName()
    {
        var prefix = sql.GetTablePrefix(additionalConnConfig);
        return $"{prefix}job_execution";
    }

    private string BuildJobExecutionInsertSql()
    {
        var t = JobExecutionTableName();
        return $@"INSERT INTO {t}
            (cluster_id, id, job_id, started_at, agent_connection_id, agent_worker_id, bucket_id, host_id, host_display_name, finalized_at, outcome_message, outcome)
            VALUES
            (@ClusterId, @Id, @JobId, @StartedAt, @AgentConnectionId, @AgentWorkerId, @BucketId, @HostId, @HostDisplayName, @FinalizedAt, @OutcomeMessage, @Outcome)";
    }

    private static DynamicParameters BuildJobExecutionParams(JobExecution execution)
    {
        var p = new DynamicParameters();
        p.Add("ClusterId", execution.ClusterId);
        p.Add("Id", execution.Id);
        p.Add("JobId", execution.JobId);
        p.Add("StartedAt", execution.StartedAt);
        p.Add("AgentConnectionId", execution.AgentConnectionId?.IdValue);
        p.Add("AgentWorkerId", execution.AgentWorkerId);
        p.Add("BucketId", execution.BucketId);
        p.Add("HostId", execution.HostId?.IdValue);
        p.Add("HostDisplayName", execution.HostId?.HostDisplayName);
        p.Add("FinalizedAt", execution.FinalizedAt);
        p.Add("OutcomeMessage", execution.OutcomeMessage);
        p.Add("Outcome", (int)execution.Outcome);
        return p;
    }

    protected (string Columns, string ValuesParams) InsertColumnsAndParams()
    {
        var cols = new[]
        {
            Col(x => x.ClusterId), Col(x => x.Id), Col(x => x.JobDefinitionId), Col(x => x.TriggerSourceType),
            Col(x => x.BucketId), Col(x => x.AgentConnectionId), Col(x => x.AgentWorkerId), Col(x => x.Priority),
            Col(x => x.ScheduledAt), Col(x => x.NextPlanExecutionAt), Col(x => x.MsgData), Col(x => x.Status),
            Col(x => x.NumberOfFailures), Col(x => x.TimeoutTicks), Col(x => x.MaxNumberOfRetries),
            Col(x => x.CreatedAt), Col(x => x.SourceId),
            Col(x => x.PartitionLockId), Col(x => x.PartitionLockExpiresAt), Col(x => x.ProcessDeadline),
            Col(x => x.ProcessStartedAt), Col(x => x.FinalizedAt),
            Col(x => x.WorkerLane), Col(x => x.Version), Col(x => x.HostId), Col(x => x.HostDisplayName)
        };
        var vals = new[]
        {
            "@ClusterId", "@Id", "@JobDefinitionId", "@TriggerSourceType",
            "@BucketId", "@AgentConnectionId", "@AgentWorkerId", "@Priority",
            "@ScheduledAt", "@NextPlanExecutionAt", "@MsgData", "@Status",
            "@NumberOfFailures", "@TimeoutTicks", "@MaxNumberOfRetries",
            "@CreatedAt", "@SourceId",
            "@PartitionLockId", "@PartitionLockExpiresAt", "@ProcessDeadline",
            "@ProcessStartedAt", "@FinalizedAt",
            "@WorkerLane", "@Version", "@HostId", "@HostDisplayName"
        };
        return (string.Join(", ", cols), string.Join(", ", vals));
    }

    private string BuildUpdateSql()
    {
        var t = TableName();
        var cVersion = Col(x => x.Version);
        var cClusterId = Col(x => x.ClusterId);
        var cId = Col(x => x.Id);
        return $@"
UPDATE {t} SET
    {UpdateSetClause()}
WHERE {cClusterId} = @ClusterId
  AND {cId} = @Id
  AND {cVersion} = @ExpectedVersion;";
    }

    protected string UpdateSetClause()
    {
        return string.Join(", ", new[]
        {
            $"{Col(x => x.JobDefinitionId)} = @JobDefinitionId",
            $"{Col(x => x.TriggerSourceType)} = @TriggerSourceType",
            $"{Col(x => x.BucketId)} = @BucketId",
            $"{Col(x => x.AgentConnectionId)} = @AgentConnectionId",
            $"{Col(x => x.AgentWorkerId)} = @AgentWorkerId",
            $"{Col(x => x.Priority)} = @Priority",
            $"{Col(x => x.NextPlanExecutionAt)} = @NextPlanExecutionAt",
            $"{Col(x => x.ScheduledAt)} = @ScheduledAt",
            $"{Col(x => x.MsgData)} = @MsgData",
            $"{Col(x => x.Status)} = @Status",
            $"{Col(x => x.NumberOfFailures)} = @NumberOfFailures",
            $"{Col(x => x.TimeoutTicks)} = @TimeoutTicks",
            $"{Col(x => x.MaxNumberOfRetries)} = @MaxNumberOfRetries",
            $"{Col(x => x.SourceId)} = @SourceId",
            $"{Col(x => x.PartitionLockId)} = @PartitionLockId",
            $"{Col(x => x.PartitionLockExpiresAt)} = @PartitionLockExpiresAt",
            $"{Col(x => x.ProcessDeadline)} = @ProcessDeadline",
            $"{Col(x => x.ProcessStartedAt)} = @ProcessStartedAt",
            $"{Col(x => x.FinalizedAt)} = @FinalizedAt",
            $"{Col(x => x.WorkerLane)} = @WorkerLane",
            $"{Col(x => x.Version)} = @Version",
            $"{Col(x => x.HostId)} = @HostId",
            $"{Col(x => x.HostDisplayName)} = @HostDisplayName"
        });
    }

    protected string SelectProjection(string jobAlias = "j", string genericEntryAlias = "e", string genericEntryValueAlias = "v")
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
            $"{jobAlias}.{Col(x => x.ScheduledAt)}",
            $"{jobAlias}.{Col(x => x.NextPlanExecutionAt)}",
            $"{jobAlias}.{Col(x => x.MsgData)}",
            $"{jobAlias}.{Col(x => x.Status)}",
            $"{jobAlias}.{Col(x => x.NumberOfFailures)}",
            $"{jobAlias}.{Col(x => x.TimeoutTicks)}",
            $"{jobAlias}.{Col(x => x.MaxNumberOfRetries)}",
            $"{jobAlias}.{Col(x => x.CreatedAt)}",
            $"{jobAlias}.{Col(x => x.SourceId)}",
            $"{jobAlias}.{Col(x => x.PartitionLockId)}",
            $"{jobAlias}.{Col(x => x.PartitionLockExpiresAt)}",
            $"{jobAlias}.{Col(x => x.ProcessDeadline)}",
            $"{jobAlias}.{Col(x => x.ProcessStartedAt)}",
            $"{jobAlias}.{Col(x => x.FinalizedAt)}",
            $"{jobAlias}.{Col(x => x.WorkerLane)}",
            $"{jobAlias}.{Col(x => x.Version)}",
            $"{jobAlias}.{Col(x => x.HostId)}",
            $"{jobAlias}.{Col(x => x.HostDisplayName)}",

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
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = j.{Col(x => x.Id)} and e.{Col(x => x.GroupId)} = @GroupId
LEFT JOIN {genericUtil.EntryValueTable(MasterGenericRecordGroupIds.JobMetadata)} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)} 
{whereSql}
{order}";
        
        return (sqlText, new Dictionary<string, object?> { { "GroupId", MasterGenericRecordGroupIds.JobMetadata } });
    }

    protected string Col(Expression<Func<JobPersistenceRecordLinearDto, object?>> prop) => sql.ColumnNameFor(prop);

    protected IList<JobPersistenceRecord> LinearListRecord(IList<JobPersistenceRecordLinearDto> list)
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
                ScheduledAt = first.ScheduledAt,
                NextPlanExecutionAt = first.NextPlanExecutionAt,
                MsgData = first.MsgData,
                Status = first.Status,
                NumberOfFailures = first.NumberOfFailures,
                TimeoutTicks = first.TimeoutTicks,
                MaxNumberOfRetries = first.MaxNumberOfRetries,
                CreatedAt = first.CreatedAt,
                SourceId = first.SourceId,
                PartitionLockId = first.PartitionLockId,
                PartitionLockExpiresAt = first.PartitionLockExpiresAt,
                ProcessDeadline = first.ProcessDeadline,
                ProcessStartedAt = first.ProcessStartedAt,
                FinalizedAt = first.FinalizedAt,
                Metadata = metadata,
                WorkerLane = first.WorkerLane,
                Version = first.Version,
                HostId = first.HostId,
                HostDisplayName = first.HostDisplayName,
            };

            result.Add(rec);
        }

        return result;
    }
    
    protected string UpdateSetClauseWithoutVersion()
    {
        return string.Join(", ", new[]
        {
            $"{Col(x => x.JobDefinitionId)} = @JobDefinitionId",
            $"{Col(x => x.TriggerSourceType)} = @TriggerSourceType",
            $"{Col(x => x.BucketId)} = @BucketId",
            $"{Col(x => x.AgentConnectionId)} = @AgentConnectionId",
            $"{Col(x => x.AgentWorkerId)} = @AgentWorkerId",
            $"{Col(x => x.Priority)} = @Priority",
            $"{Col(x => x.NextPlanExecutionAt)} = @NextPlanExecutionAt",
            $"{Col(x => x.ScheduledAt)} = @ScheduledAt",
            $"{Col(x => x.MsgData)} = @MsgData",
            $"{Col(x => x.Status)} = @Status",
            $"{Col(x => x.NumberOfFailures)} = @NumberOfFailures",
            $"{Col(x => x.TimeoutTicks)} = @TimeoutTicks",
            $"{Col(x => x.MaxNumberOfRetries)} = @MaxNumberOfRetries",
            $"{Col(x => x.SourceId)} = @SourceId",
            $"{Col(x => x.PartitionLockId)} = @PartitionLockId",
            $"{Col(x => x.PartitionLockExpiresAt)} = @PartitionLockExpiresAt",
            $"{Col(x => x.ProcessDeadline)} = @ProcessDeadline",
            $"{Col(x => x.ProcessStartedAt)} = @ProcessStartedAt",
            $"{Col(x => x.FinalizedAt)} = @FinalizedAt",
            $"{Col(x => x.WorkerLane)} = @WorkerLane",
            $"{Col(x => x.HostId)} = @HostId",
            $"{Col(x => x.HostDisplayName)} = @HostDisplayName"
        });
    }

    protected class JobPersistenceRecordLinearDto : JobPersistenceRecord
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