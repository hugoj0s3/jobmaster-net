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
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils.Extensions;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Models.Jobs;
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
            var rec = SqlJobPersistenceConvertUtil.ToPersistence(jobRaw);
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
            var rec = SqlJobPersistenceConvertUtil.ToPersistence(jobRaw);
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
            var rec = SqlJobPersistenceConvertUtil.ToPersistence(jobRaw);
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
            var rec = SqlJobPersistenceConvertUtil.ToPersistence(jobRaw);
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
        var rows = (await conn.QueryAsync<SqlJobExecutionPersistenceRecord>(sqlText,
            new { ClusterId = ClusterConnConfig.ClusterId, JobId = jobId })).ToList();

        return rows.Select(SqlJobPersistenceConvertUtil.FromPersistence).ToList();
    }

    public async Task<IList<JobExecution>> QueryJobExecutionsForJobsAsync(IList<Guid> jobIds)
    {
        if (jobIds.Count == 0) return new List<JobExecution>();

        var t = JobExecutionTableName();
        var maxBatch = OperationThrottlerSettingsTemplateFactory.GetMasterMaxBatchSize(ClusterConnConfig.RepositoryTypeId);
        var results = new List<JobExecution>();

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        foreach (var idsPartition in jobIds.Partition(maxBatch).ToList())
        {
            var inClause = sql.InClauseFor("job_id", "@JobIds");
            var sqlText = $@"
SELECT cluster_id, id, job_id, started_at, agent_connection_id, agent_worker_id, bucket_id, host_id, host_display_name, finalized_at, outcome_message, outcome
FROM {t}
WHERE cluster_id = @ClusterId AND {inClause}";

            var rows = await conn.QueryAsync<SqlJobExecutionPersistenceRecord>(sqlText,
                new { ClusterId = ClusterConnConfig.ClusterId, JobIds = idsPartition });
            results.AddRange(rows.Select(SqlJobPersistenceConvertUtil.FromPersistence));
        }

        return results;
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
        using var conn = connManager.Open(connString, additionalConnConfig);
        var (sqlText, args) = BuildQuerySql(queryCriteria);
        var linearRows = conn.Query<SqlJobPersistenceRecordLinearDto>(sqlText, args).ToList();
        var rows = LinearListRecord(linearRows);
        return rows.Select(SqlJobPersistenceConvertUtil.FromPersistence).ToList();
    }

    public async Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria queryCriteria)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var (sqlText, args) = BuildQuerySql(queryCriteria);
        var linearRows = (await conn.QueryAsync<SqlJobPersistenceRecordLinearDto>(sqlText, args)).ToList();
        var rows = LinearListRecord(linearRows);
        return rows.Select(SqlJobPersistenceConvertUtil.FromPersistence).ToList();
    }

    public JobRawModel? Get(Guid jobId)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        var (sqlText, args) = BuildGetSql(jobId);
        var linearRows = conn.Query<SqlJobPersistenceRecordLinearDto>(sqlText, args).ToList();
        var rows = LinearListRecord(linearRows);

        return rows.Select(SqlJobPersistenceConvertUtil.FromPersistence).SingleOrDefault();
    }

    public async Task<JobRawModel?> GetAsync(Guid jobId)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var (sqlText, args) = BuildGetSql(jobId);

        var linearRows = (await conn.QueryAsync<SqlJobPersistenceRecordLinearDto>(sqlText, args)).ToList();
        var rows = LinearListRecord(linearRows);

        return rows.Select(SqlJobPersistenceConvertUtil.FromPersistence).SingleOrDefault();
    }

    public long Count(JobQueryCriteria queryCriteria)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
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

    public async Task<JobProbeResult> ProbeForAcquireAsync(JobQueryCriteria queryCriteria)
    {
        if (queryCriteria.MetadataFilters.Count > 0)
            throw new NotSupportedException("ProbeForAcquire does not support MetadataFilters.");

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        
        var (whereSql, args) = BuildWhere(queryCriteria, isLocked: false);
        var t = TableName();
        var sqlText = @$"
SELECT COUNT(*) as Count, MIN(j.{Col(x => x.NextPlanExecutionAt)}) as MinNextPlanExecutionAt
FROM {t} j
{whereSql}";
        return await conn.QuerySingleAsync<JobProbeResult>(sqlText, args);
    }

    public virtual async Task BulkUpdateAsync(BulkJobUpdateRequest request)
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

    // ANSI-portable fallback: one single-row UPDATE per job. Providers override this with a
    // dialect-specific multi-row UPDATE (see PostgresMasterJobsRepository, SqlServerMasterJobsRepository,
    // MySqlMasterJobsRepository) since there's no ANSI-standard syntax for "UPDATE with per-row differing
    // values from a value list".
    public virtual async Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs)
    {
        if (jobs.Count == 0) return Array.Empty<JobRawModel>();

        var t = TableName();
        var cId = Col(x => x.Id);
        var cClusterId = Col(x => x.ClusterId);
        var cVersion = Col(x => x.Version);
        var setClause = $"{UpdateSetClauseWithoutVersion()}, {cVersion} = @Version";
        var sqlText = $"UPDATE {t} SET {setClause} WHERE {cClusterId} = @ClusterId AND {cId} = @Id AND {cVersion} = @ExpectedVersion";

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
                var rec = SqlJobPersistenceConvertUtil.ToPersistence(job);
                var expectedVersion = rec.Version;
                rec.Version = newVersions[job.Id];
                var dp = new DynamicParameters(rec);
                dp.Add("ExpectedVersion", expectedVersion);
                var rowsAffected = await conn.ExecuteAsync(sqlText, dp, trans);
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

    /// <summary>
    /// Binds one job's bulk-update row (id + the 21 <see cref="UpdateSetClauseWithoutVersion"/> columns
    /// + a freshly generated version + the version the row is expected to currently be at) into
    /// <paramref name="p"/> using suffixed parameter names (<c>Id_{i}</c>, <c>Status_{i}</c>, ...),
    /// matching the indexed-parameter convention already used by <see cref="BuildBulkInsertIfNotExistsSql"/>.
    /// Shared by the per-provider bulk-UPDATE overrides so the parameter list only needs to be kept in
    /// sync with <see cref="SqlJobPersistenceRecord"/> once. <paramref name="expectedVersion"/> must be
    /// captured by the caller BEFORE overwriting <paramref name="rec"/>'s <c>Version</c> with the new
    /// one -- it's what enforces optimistic concurrency: a row whose current version doesn't match is
    /// left untouched instead of being silently overwritten.
    /// </summary>
    protected static void AddBulkUpdateRowParams(DynamicParameters p, int i, SqlJobPersistenceRecord rec, string? expectedVersion)
    {
        p.Add($"Id_{i}", rec.Id, DbType.Guid);
        p.Add($"JobDefinitionId_{i}", rec.JobDefinitionId, DbType.String);
        p.Add($"TriggerSourceType_{i}", rec.TriggerSourceType, DbType.Int32);
        p.Add($"BucketId_{i}", rec.BucketId, DbType.String);
        p.Add($"AgentConnectionId_{i}", rec.AgentConnectionId, DbType.String);
        p.Add($"AgentWorkerId_{i}", rec.AgentWorkerId, DbType.String);
        p.Add($"Priority_{i}", rec.Priority, DbType.Int32);
        p.Add($"NextPlanExecutionAt_{i}", rec.NextPlanExecutionAt, DbType.DateTime);
        p.Add($"ScheduledAt_{i}", rec.ScheduledAt, DbType.DateTime);
        p.Add($"MsgData_{i}", rec.MsgData, DbType.String);
        p.Add($"Status_{i}", rec.Status, DbType.Int32);
        p.Add($"NumberOfFailures_{i}", rec.NumberOfFailures, DbType.Int32);
        p.Add($"TimeoutTicks_{i}", rec.TimeoutTicks, DbType.Int64);
        p.Add($"MaxNumberOfRetries_{i}", rec.MaxNumberOfRetries, DbType.Int32);
        p.Add($"SourceId_{i}", rec.SourceId, DbType.Guid);
        p.Add($"PartitionLockId_{i}", rec.PartitionLockId, DbType.Guid);
        p.Add($"PartitionLockExpiresAt_{i}", rec.PartitionLockExpiresAt, DbType.DateTime);
        p.Add($"ProcessDeadline_{i}", rec.ProcessDeadline, DbType.DateTime);
        p.Add($"ProcessStartedAt_{i}", rec.ProcessStartedAt, DbType.DateTime);
        p.Add($"FinalizedAt_{i}", rec.FinalizedAt, DbType.DateTime);
        p.Add($"WorkerLane_{i}", rec.WorkerLane, DbType.String);
        p.Add($"Version_{i}", rec.Version, DbType.String);
        p.Add($"HostId_{i}", rec.HostId, DbType.String);
        p.Add($"HostDisplayName_{i}", rec.HostDisplayName, DbType.String);
        p.Add($"ExpectedVersion_{i}", expectedVersion, DbType.String);
    }

    public async Task<IList<Guid>> PurgeFinalizedAsync(DateTime cutoffUtc, int limit)
    {
        if (limit <= 0) throw new ArgumentException("limit must be > 0", nameof(limit));

        var t = TableName();
        var cId = Col(x => x.Id);
        var cStatus = Col(x => x.Status);
        var cFinalizedAt = Col(x => x.FinalizedAt);

        var selectSql = new StringBuilder($@"SELECT {cId} FROM {t}
WHERE {Col(x => x.ClusterId)} = @ClusterId
  AND {cStatus} IN (@Succeeded, @Failed, @Cancelled, @Aborted)
  AND {cFinalizedAt} IS NOT NULL
  AND {cFinalizedAt} <= @CutoffUtc
ORDER BY {cFinalizedAt} ASC, {cId} ASC");
        selectSql.Append('\n');
        selectSql.Append(sql.OffsetQueryFor(limit, 0));

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var ids = (await conn.QueryAsync<Guid>(selectSql.ToString(), new
        {
            ClusterId = ClusterConnConfig.ClusterId,
            CutoffUtc = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Utc),
            Succeeded = (int)JobMasterJobStatus.Succeeded,
            Failed = (int)JobMasterJobStatus.Failed,
            Cancelled = (int)JobMasterJobStatus.Cancelled,
            Aborted = (int)JobMasterJobStatus.Aborted,
        })).ToList();

        if (ids.Count == 0) return ids;

        await DeleteByIdsAsync(ids);
        return ids;
    }

    public async Task<IList<JobRawModel>> QueryFinalizedToPurgeAsync(DateTime cutoffUtc, int limit)
    {
        if (limit <= 0) throw new ArgumentException("limit must be > 0", nameof(limit));

        var t = TableName();
        var selectCols = SelectProjection();
        var cId = Col(x => x.Id);
        var cClusterId = Col(x => x.ClusterId);
        var cStatus = Col(x => x.Status);
        var cFinalizedAt = Col(x => x.FinalizedAt);

        var whereSql = $@"WHERE j.{cClusterId} = @ClusterId
  AND j.{cStatus} IN (@Succeeded, @Failed, @Cancelled, @Aborted)
  AND j.{cFinalizedAt} IS NOT NULL
  AND j.{cFinalizedAt} <= @CutoffUtc";
        var order = $"ORDER BY j.{cFinalizedAt} ASC, j.{cId} ASC";
        var offsetClause = sql.OffsetQueryFor(limit, 0);

        var queryText = $@"
WITH jobs_page AS (
    SELECT j.*
    FROM {t} j
    {whereSql}
    {order}
    {offsetClause}
)
SELECT {selectCols}
FROM jobs_page j
{order}";

        var args = new
        {
            ClusterId = ClusterConnConfig.ClusterId,
            CutoffUtc = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Utc),
            Succeeded = (int)JobMasterJobStatus.Succeeded,
            Failed = (int)JobMasterJobStatus.Failed,
            Cancelled = (int)JobMasterJobStatus.Cancelled,
            Aborted = (int)JobMasterJobStatus.Aborted,
        };

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var linearRows = (await conn.QueryAsync<SqlJobPersistenceRecordLinearDto>(queryText, args)).ToList();
        var rows = LinearListRecord(linearRows);
        return rows.Select(SqlJobPersistenceConvertUtil.FromPersistence).ToList();
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

                // Delete Job Executions
                var execTable = JobExecutionTableName();
                var execInClause = sql.InClauseFor("job_id", "@Ids");
                var deleteExecSql = $"DELETE FROM {execTable} WHERE {cClusterId} = @ClusterId AND {execInClause}";
                await conn.ExecuteAsync(deleteExecSql, new { ClusterId = ClusterConnConfig.ClusterId, Ids = idsPartition }, tx);
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

    public virtual async Task<IList<Guid>> BulkInsertIfNotExistsAsync(IList<JobRawModel> jobs, IList<JobExecution> jobExecutions)
    {
        if (jobs.Count == 0) return new List<Guid>();

        var t = TableName();
        var maxBatch = OperationThrottlerSettingsTemplateFactory.GetMasterMaxBatchSize(ClusterConnConfig.RepositoryTypeId);
        var executionsByJobId = jobExecutions.ToLookup(e => e.JobId);
        var insertedIds = new List<Guid>();

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            for (var offset = 0; offset < jobs.Count; offset += maxBatch)
            {
                var count = Math.Min(maxBatch, jobs.Count - offset);
                var batch = jobs.Skip(offset).Take(count).ToList();

                var (existsSql, existsParams) = genericUtil.BuildQueryExistingIdsSql(t, Col(x => x.Id), batch.Select(j => j.Id));
                var existingIds = new HashSet<Guid>(await conn.QueryAsync<Guid>(existsSql, existsParams, tx));
                var newJobs = batch.Where(j => !existingIds.Contains(j.Id)).ToList();

                var (sqlText, dynParams) = BuildBulkInsertIfNotExistsSql(t, jobs, offset, count);
                await conn.ExecuteAsync(sqlText, dynParams, tx);

                if (newJobs.Count > 0)
                {
                    insertedIds.AddRange(newJobs.Select(j => j.Id));

                    var newEntries = newJobs
                        .Select(j => genericUtil.MapToSqlEntry(SqlJobPersistenceConvertUtil.ToPersistence(j).Metadata!))
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

                    var executionsToInsert = newJobs.SelectMany(j => executionsByJobId[j.Id]).ToList();
                    if (executionsToInsert.Count > 0)
                    {
                        var (execSqlText, execDynParams) = BuildBulkInsertJobExecutionsSql(executionsToInsert);
                        await conn.ExecuteAsync(execSqlText, execDynParams, tx);
                    }
                }
            }
            tx.Commit();
            return insertedIds;
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
        string t, IList<JobRawModel> jobs, int offset, int count)
    {
        var (cols, _) = InsertColumnsAndParams();
        var cId = Col(x => x.Id);
        var cClusterId = Col(x => x.ClusterId);

        var dynParams = new DynamicParameters();
        var selects = new List<string>();

        for (var i = 0; i < count; i++)
        {
            var job = jobs[offset + i];
            var rec = SqlJobPersistenceConvertUtil.ToPersistence(job);
            rec.Version = JobMasterRandomUtil.NewGuid4().ToString("N").ToLowerInvariant();

            selects.Add($@"SELECT @ClusterId_{i}, @Id_{i}, @JobDefinitionId_{i}, @TriggerSourceType_{i}, @BucketId_{i}, @AgentConnectionId_{i}, @AgentWorkerId_{i}, @Priority_{i}, @ScheduledAt_{i}, @NextPlanExecutionAt_{i}, @MsgData_{i}, @MetadataJson_{i}, @Status_{i}, @NumberOfFailures_{i}, @TimeoutTicks_{i}, @MaxNumberOfRetries_{i}, @CreatedAt_{i}, @SourceId_{i}, @PartitionLockId_{i}, @PartitionLockExpiresAt_{i}, @ProcessDeadline_{i}, @ProcessStartedAt_{i}, @FinalizedAt_{i}, @WorkerLane_{i}, @Version_{i}, @HostId_{i}, @HostDisplayName_{i}
WHERE NOT EXISTS (SELECT 1 FROM {t} WHERE {cClusterId} = @ExistsClusterId_{i} AND {cId} = @ExistsId_{i})");

            dynParams.Add($"ClusterId_{i}", rec.ClusterId, DbType.String);
            dynParams.Add($"Id_{i}", rec.Id, DbType.Guid);
            dynParams.Add($"JobDefinitionId_{i}", rec.JobDefinitionId, DbType.String);
            dynParams.Add($"TriggerSourceType_{i}", rec.TriggerSourceType, DbType.Int32);
            dynParams.Add($"BucketId_{i}", rec.BucketId, DbType.String);
            dynParams.Add($"AgentConnectionId_{i}", rec.AgentConnectionId, DbType.String);
            dynParams.Add($"AgentWorkerId_{i}", rec.AgentWorkerId, DbType.String);
            dynParams.Add($"Priority_{i}", rec.Priority, DbType.Int32);
            dynParams.Add($"ScheduledAt_{i}", rec.ScheduledAt, DbType.DateTime);
            dynParams.Add($"NextPlanExecutionAt_{i}", rec.NextPlanExecutionAt, DbType.DateTime);
            dynParams.Add($"MsgData_{i}", rec.MsgData, DbType.String);
            dynParams.Add($"MetadataJson_{i}", rec.MetadataJson, DbType.String);
            dynParams.Add($"Status_{i}", rec.Status, DbType.Int32);
            dynParams.Add($"NumberOfFailures_{i}", rec.NumberOfFailures, DbType.Int32);
            dynParams.Add($"TimeoutTicks_{i}", rec.TimeoutTicks, DbType.Int64);
            dynParams.Add($"MaxNumberOfRetries_{i}", rec.MaxNumberOfRetries, DbType.Int32);
            dynParams.Add($"CreatedAt_{i}", rec.CreatedAt, DbType.DateTime);
            dynParams.Add($"SourceId_{i}", rec.SourceId, DbType.Guid);
            dynParams.Add($"PartitionLockId_{i}", rec.PartitionLockId, DbType.Guid);
            dynParams.Add($"PartitionLockExpiresAt_{i}", rec.PartitionLockExpiresAt, DbType.DateTime);
            dynParams.Add($"ProcessDeadline_{i}", rec.ProcessDeadline, DbType.DateTime);
            dynParams.Add($"ProcessStartedAt_{i}", rec.ProcessStartedAt, DbType.DateTime);
            dynParams.Add($"FinalizedAt_{i}", rec.FinalizedAt, DbType.DateTime);
            dynParams.Add($"WorkerLane_{i}", rec.WorkerLane, DbType.String);
            dynParams.Add($"Version_{i}", rec.Version, DbType.String);
            dynParams.Add($"HostId_{i}", rec.HostId, DbType.String);
            dynParams.Add($"HostDisplayName_{i}", rec.HostDisplayName, DbType.String);
            dynParams.Add($"ExistsClusterId_{i}", rec.ClusterId, DbType.String);
            dynParams.Add($"ExistsId_{i}", rec.Id, DbType.Guid);
        }

        var sqlText = $"INSERT INTO {t} ({cols})\n{string.Join("\nUNION ALL\n", selects)};";
        return (sqlText, dynParams);
    }

    public virtual async Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, Guid partitionLockId,
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
            // isLocked: false restricts the inner SELECT to unlocked rows only, so the
            // LIMIT picks genuinely acquirable candidates. Without it, already-locked rows
            // fill the LIMIT and the outer unlockedGuard discards them silently, returning
            // 0 rows even when unlocked rows exist further in the result set.
            var (whereSql, args) = BuildWhere(queryCriteria, isLocked: false);
            var needsMetadataJoin = queryCriteria.MetadataFilters is { Count: > 0 };
            var queryIdsSql = 
                BuildQueryIdsToLockSql(whereSql, needsMetadataJoin, queryCriteria.CountLimit, queryCriteria.Offset, queryCriteria.SortBy);
            var updateSql = $@"
UPDATE {t} {UpdateToLockTableHint}
SET {Col(x => x.PartitionLockId)} = @PartitionLockId,
    {Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    {Col(x => x.Version)} = {sql.GenerateVersionSql()}
where {Col(x => x.ClusterId)} = @ClusterId
  AND {Col(x => x.Id)} in ({queryIdsSql})
  AND {unlockedGuard}
  ;";
            var args2 = new Dictionary<string, object?>(args);
            args2.Add("LockNowUtc", nowUtcWithSkew);
            args2.Add("LockExpiresAt", expiresAtUtcKind);
            args2.Add("PartitionLockId", partitionLockId);

            var rowsAffected = await conn.ExecuteAsync(updateSql, args2, trans);

            trans.Commit();

            if (rowsAffected == 0)
            {
                return new List<JobRawModel>();
            }

            using var conn2 = await connManager.OpenAsync(connString, additionalConnConfig);
            var result = await QueryLockedJobsAsync(partitionLockId, nowUtcWithSkew, conn2);
            return result;
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    protected async Task<IList<JobRawModel>> QueryLockedJobsAsync(
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
WHERE j.{cClusterId} = @ClusterId
  AND j.{cPartitionLockId} = @PartitionLockId
  AND j.{cPartitionLockExpiresAt} > @NowUtcWithSkew";

        var args = new Dictionary<string, object?>
        {
            { "ClusterId", ClusterConnConfig.ClusterId },
            { "PartitionLockId", partitionLockId },
            { "NowUtcWithSkew", nowUtcWithSkew }
        };

        var linearRows = (await cnn2.QueryAsync<SqlJobPersistenceRecordLinearDto>(sqlText, args)).ToList();
        var rows = LinearListRecord(linearRows);
        return rows.Select(SqlJobPersistenceConvertUtil.FromPersistence).ToList();
    }
    
    protected virtual string UpdateToLockTableHint => string.Empty;
    
    protected virtual bool IsDupeViolation(Guid jobId, Exception ex)
    {
        return knownExceptionIdentifier.Identify(this.ClusterConnConfig.RepositoryTypeId, ex) == JobMasterKnownExceptionId.DuplicateKey;
    }

    // SQL builders
    private (string, object) BuildGetSql(Guid jobId)
    {
        var selectCols = SelectProjection();
        var sqlText = $@"
SELECT {selectCols}
FROM {TableName()} j
WHERE j.{Col(x => x.ClusterId)} = @ClusterId AND j.{Col(x => x.Id)} = @Id
";
        var args = new { ClusterId = ClusterConnConfig.ClusterId, Id = jobId };
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


    private (string, object) BuildQuerySql(JobQueryCriteria c, int? partitionLockId = null, bool? isLocked = null)
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

        // MetadataFilters (if any) compile into an EXISTS(...) inside whereSql that correlates
        // against alias "e" (genericUtil.BuildWhereClause's entryTableAlias) -- that EXISTS only
        // defines its own value-table alias internally, so "e" itself still needs to come from a
        // join in this outer query. Retrieval no longer needs it (MetadataJson replaces that), so
        // this join -- unlike before -- is now conditional on filters actually being present.
        var needsMetadataJoin = c.MetadataFilters is { Count: > 0 };
        if (needsMetadataJoin)
        {
            args["GroupId"] = MasterGenericRecordGroupIds.JobMetadata;
        }
        var metadataJoin = needsMetadataJoin
            ? $"LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = j.{Col(x => x.Id)} AND e.{Col(x => x.GroupId)} = @GroupId"
            : string.Empty;

        if (c.CountLimit > 0)
        {
            var offsetClause = sql.OffsetQueryFor(c.CountLimit, c.Offset);

            var queryText = $@"
WITH jobs_page AS (
    SELECT j.*
    FROM {t} j
    {metadataJoin}
    {whereSql}
    {order}
    {offsetClause}
)
SELECT {selectCols}
FROM jobs_page j
{order}";

            return (queryText, args);
        }

        var (queryTextNoPaging, queryArgsNoPaging) = BuildQuery(t, selectCols, whereSql, metadataJoin, order);
        var concatedArgsNoPaging = args.Concat(queryArgsNoPaging).ToDictionary(x => x.Key, x => x.Value);
        return (queryTextNoPaging, concatedArgsNoPaging);
    }

    protected (string whereSql, Dictionary<string, object?> args) BuildWhere(
        JobQueryCriteria c, 
        int? partitionLockId = null, 
        bool? isLocked = null)
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

        if (!string.IsNullOrEmpty(c.BucketId))
        {
            where.Add($"j.{Col(x => x.BucketId)} = @BucketId");
            args.Add("BucketId", c.BucketId);
        }

        if (c.ExcludeBucketIds is { Count: > 0 })
        {
            var notInClause = sql.InClauseFor($"j.{Col(x => x.BucketId)}", "@ExcludeBucketIds");
            where.Add($"NOT ({notInClause})");
            args.Add("ExcludeBucketIds", c.ExcludeBucketIds.ToArray());
        }

        if (c.Priority.HasValue)
        {
            where.Add($"j.{Col(x => x.Priority)} = @Priority");
            args.Add("Priority", (int)c.Priority.Value);
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

    // Bulk variant of BuildJobExecutionInsertSql/BuildJobExecutionParams -- used by BulkInsertIfNotExistsAsync
    // to copy JobExecutions for newly-inserted jobs during archiving/migration. No existence guard needed:
    // the caller only ever passes executions belonging to jobs it just confirmed are new to this cluster.
    private (string sqlText, DynamicParameters dynParams) BuildBulkInsertJobExecutionsSql(IList<JobExecution> executions)
    {
        var t = JobExecutionTableName();
        const string cols = "cluster_id, id, job_id, started_at, agent_connection_id, agent_worker_id, bucket_id, host_id, host_display_name, finalized_at, outcome_message, outcome";
        var sb = new StringBuilder($"INSERT INTO {t} ({cols}) VALUES \n");
        var dynParams = new DynamicParameters();

        for (var i = 0; i < executions.Count; i++)
        {
            var e = executions[i];
            sb.Append($"(@ClusterId_{i}, @Id_{i}, @JobId_{i}, @StartedAt_{i}, @AgentConnectionId_{i}, @AgentWorkerId_{i}, @BucketId_{i}, @HostId_{i}, @HostDisplayName_{i}, @FinalizedAt_{i}, @OutcomeMessage_{i}, @Outcome_{i})");
            if (i < executions.Count - 1) sb.Append(",\n");

            dynParams.Add($"ClusterId_{i}", e.ClusterId);
            dynParams.Add($"Id_{i}", e.Id);
            dynParams.Add($"JobId_{i}", e.JobId);
            dynParams.Add($"StartedAt_{i}", e.StartedAt);
            dynParams.Add($"AgentConnectionId_{i}", e.AgentConnectionId?.IdValue);
            dynParams.Add($"AgentWorkerId_{i}", e.AgentWorkerId);
            dynParams.Add($"BucketId_{i}", e.BucketId);
            dynParams.Add($"HostId_{i}", e.HostId?.IdValue);
            dynParams.Add($"HostDisplayName_{i}", e.HostId?.HostDisplayName);
            dynParams.Add($"FinalizedAt_{i}", e.FinalizedAt);
            dynParams.Add($"OutcomeMessage_{i}", e.OutcomeMessage);
            dynParams.Add($"Outcome_{i}", (int)e.Outcome);
        }

        return (sb.ToString(), dynParams);
    }

    protected (string Columns, string ValuesParams) InsertColumnsAndParams()
    {
        var cols = new[]
        {
            Col(x => x.ClusterId), Col(x => x.Id), Col(x => x.JobDefinitionId), Col(x => x.TriggerSourceType),
            Col(x => x.BucketId), Col(x => x.AgentConnectionId), Col(x => x.AgentWorkerId), Col(x => x.Priority),
            Col(x => x.ScheduledAt), Col(x => x.NextPlanExecutionAt), Col(x => x.MsgData), Col(x => x.MetadataJson), Col(x => x.Status),
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
            "@ScheduledAt", "@NextPlanExecutionAt", "@MsgData", "@MetadataJson", "@Status",
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

    // Deliberately excludes MetadataJson/Metadata: metadata is set once at insert time and is
    // immutable through Update/Upsert/BulkUpdateAsync (confirmed by RepoConformance's
    // Update_ShouldPersistChanges / BulkUpdate_Models_ShouldPersist_AllChanges, which assert the
    // original metadata survives even when a caller passes a JobRawModel with different Metadata).
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

    // No more generic-record entry/value LEFT JOIN columns here -- Metadata is read straight off
    // MetadataJson (see SqlJobPersistenceRecord.MetadataJson), so every caller of this projection
    // (Get/GetAsync/Query/QueryAsync/QueryLockedJobsAsync/QueryFinalizedToPurgeAsync) is a plain
    // single-table read now, no join, no row multiplication.
    protected string SelectProjection(string jobAlias = "j")
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
            $"{jobAlias}.{Col(x => x.MetadataJson)}",
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
            $"{jobAlias}.{Col(x => x.HostDisplayName)}"
        });
    }

    private (string sqlText, IDictionary<string, object?> args) BuildQuery(string jobTableName, string selectCols, string whereSql, string metadataJoin, string order)
    {
        var sqlText =
            $@"
SELECT {selectCols}
FROM {jobTableName} j
{metadataJoin}
{whereSql}
{order}";

        return (sqlText, new Dictionary<string, object?>());
    }

    protected string Col(Expression<Func<SqlJobPersistenceRecordLinearDto, object?>> prop) => sql.ColumnNameFor(prop);

    // Named "Linear" from when this flattened a LEFT JOIN's (job, metadata-key) rows back into one
    // SqlJobPersistenceRecord per job. That join is gone -- Metadata now comes straight off
    // MetadataJson (no reconstruction needed) -- so every row is already exactly one job, and this
    // is a 1:1 map. Kept as its own step (rather than casting SqlJobPersistenceRecordLinearDto
    // directly) since it's still the place callers get a plain SqlJobPersistenceRecord back.
    protected IList<SqlJobPersistenceRecord> LinearListRecord(IList<SqlJobPersistenceRecordLinearDto> list)
    {
        return list.Select(row => new SqlJobPersistenceRecord
        {
            ClusterId = row.ClusterId,
            Id = row.Id,
            JobDefinitionId = row.JobDefinitionId,
            TriggerSourceType = row.TriggerSourceType,
            BucketId = row.BucketId,
            AgentConnectionId = row.AgentConnectionId,
            AgentWorkerId = row.AgentWorkerId,
            Priority = row.Priority,
            ScheduledAt = row.ScheduledAt,
            NextPlanExecutionAt = row.NextPlanExecutionAt,
            MsgData = row.MsgData,
            MetadataJson = row.MetadataJson,
            Status = row.Status,
            NumberOfFailures = row.NumberOfFailures,
            TimeoutTicks = row.TimeoutTicks,
            MaxNumberOfRetries = row.MaxNumberOfRetries,
            CreatedAt = row.CreatedAt,
            SourceId = row.SourceId,
            PartitionLockId = row.PartitionLockId,
            PartitionLockExpiresAt = row.PartitionLockExpiresAt,
            ProcessDeadline = row.ProcessDeadline,
            ProcessStartedAt = row.ProcessStartedAt,
            FinalizedAt = row.FinalizedAt,
            WorkerLane = row.WorkerLane,
            Version = row.Version,
            HostId = row.HostId,
            HostDisplayName = row.HostDisplayName,
        }).ToList<SqlJobPersistenceRecord>();
    }
    
    // Deliberately excludes MetadataJson/Metadata -- see UpdateSetClause's comment; the same
    // immutability contract applies to BulkUpdateAsync.
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

    protected class SqlJobPersistenceRecordLinearDto : SqlJobPersistenceRecord
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