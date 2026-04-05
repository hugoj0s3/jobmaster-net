using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Dapper;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.SqlBase.Connections;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
using JobMaster.SqlBase.Scripts;
using Npgsql;

namespace JobMaster.Postgres.Master;

internal class PostgresMasterJobsRepository : SqlMasterJobsRepository
{
    public PostgresMasterJobsRepository(JobMasterClusterConnectionConfig clusterConnectionConfig, IDbConnectionManager connectionManager) : 
        base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;

    public override async Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, int partitionLockId, DateTime expiresAtUtc)
    {
        if (partitionLockId <= 0) throw new ArgumentException("partitionLockId must be > 0", nameof(partitionLockId));
        if (queryCriteria == null) throw new ArgumentNullException(nameof(queryCriteria));

        var nowUtcWithSkew = JobMasterConstants.NowUtcWithSkewTolerance();

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig, ReadIsolationLevel.Consistent);
        using var tx = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
        try
        {
            var (whereSql, whereArgs) = BuildWhere(queryCriteria);
            var t = TableName();

            var cId = Col(x => x.Id);
            var cClusterId = Col(x => x.ClusterId);
            var cNextPlanExecutionAt = Col(x => x.NextPlanExecutionAt);

            var unlockedGuard = $"(j.{Col(x => x.PartitionLockId)} IS NULL OR j.{Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
            var defaultOrderByClause = $" ORDER BY j.{cNextPlanExecutionAt} ASC";
            var orderBy = SqlOrderByUtil.BuildOrderByClause(queryCriteria.SortBy, "j", defaultOrderByClause);

            var offsetClause = string.Empty;
            if (queryCriteria.CountLimit > 0)
            {
                offsetClause = "\n" + sql.OffsetQueryFor(queryCriteria.CountLimit, queryCriteria.Offset);
            }

            var selectCols = SelectProjection();

            var sqlText = $@"
WITH candidates AS (
    SELECT j.{cId} AS id
    FROM {t} j
    {whereSql} AND {unlockedGuard}
    {orderBy}
    {offsetClause}
), locked AS (
    UPDATE {t} j
    SET {Col(x => x.PartitionLockId)} = @PartitionLockId,
        {Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
        {Col(x => x.Version)} = {sql.GenerateVersionSql()}
    FROM candidates c
    WHERE j.{cClusterId} = @ClusterId
      AND j.{cId} = c.id
      AND {unlockedGuard}
    RETURNING j.*
)
SELECT {selectCols}
FROM locked j
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = j.{cId} and e.{Col(x => x.GroupId)} = @GroupId
LEFT JOIN {genericUtil.EntryValueTable(MasterGenericRecordGroupIds.JobMetadata)} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)}
{orderBy};";

            var args = new Dictionary<string, object?>();
            foreach (var kv in whereArgs) args[kv.Key] = kv.Value;
            args["ClusterId"] = ClusterConnConfig.ClusterId;
            args["PartitionLockId"] = partitionLockId;
            args["LockExpiresAt"] = DateTime.SpecifyKind(expiresAtUtc, DateTimeKind.Utc);
            args["LockNowUtc"] = nowUtcWithSkew;
            args["GroupId"] = MasterGenericRecordGroupIds.JobMetadata;

            var linearRows = (await conn.QueryAsync<JobPersistenceRecordLinearDto>(sqlText, args, tx)).ToList();
            var records = LinearListRecord(linearRows);
            var result = records.Select(JobRawModel.RecoverFromDb).ToList();

            tx.Commit();
            return result;
        }
        catch
        {
            tx.SafeRollback();
            throw;
        }
    }

    public override void Upsert(JobRawModel jobRaw)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var rec = JobRawModel.ToPersistence(jobRaw);
            var expectedVersion = rec.Version;
            rec.Version = Guid.NewGuid().ToString("N").ToLowerInvariant();

            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = conn.Execute(BuildUpsertSql(), dp, trans);

            if (rowsAffected == 0)
            {
                var exists = conn.ExecuteScalar<bool>(
                    $"SELECT 1 FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id",
                    new { rec.ClusterId, rec.Id }, trans);
                if (exists)
                    throw new JobMasterVersionConflictException(jobRaw.Id, "Job", expectedVersion);
            }

            if (rec.Metadata is not null)
            {
                var sqlEntry = genericUtil.MapToSqlEntry(rec.Metadata);
                var (updateEntrySql, entryParams) = genericUtil.BuildUpdateEntrySql(sqlEntry);
                if (conn.Execute(updateEntrySql, entryParams, trans) == 0)
                {
                    var (insertEntrySql, insertEntryParams) = genericUtil.BuildInsertEntrySql(sqlEntry);
                    conn.Execute(insertEntrySql, insertEntryParams, trans);
                }
                var deleteValuesSql = genericUtil.BuildDeleteValuesSql(MasterGenericRecordGroupIds.JobMetadata);
                conn.Execute(deleteValuesSql, new { RecordUniqueId = sqlEntry.RecordUniqueId }, trans);
                var (insertValuesSql, paramRows) = genericUtil.BuildInsertEntryValuesSql(sqlEntry);
                conn.Execute(insertValuesSql, paramRows, trans);
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

    public override async Task UpsertAsync(JobRawModel jobRaw)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var rec = JobRawModel.ToPersistence(jobRaw);
            var expectedVersion = rec.Version;
            rec.Version = Guid.NewGuid().ToString("N").ToLowerInvariant();

            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = await conn.ExecuteAsync(BuildUpsertSql(), dp, trans);

            if (rowsAffected == 0)
            {
                var exists = await conn.ExecuteScalarAsync<bool>(
                    $"SELECT 1 FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id",
                    new { rec.ClusterId, rec.Id }, trans);
                if (exists)
                    throw new JobMasterVersionConflictException(jobRaw.Id, "Job", expectedVersion);
            }

            if (rec.Metadata is not null)
            {
                var sqlEntry = genericUtil.MapToSqlEntry(rec.Metadata);
                var (updateEntrySql, entryParams) = genericUtil.BuildUpdateEntrySql(sqlEntry);
                if (await conn.ExecuteAsync(updateEntrySql, entryParams, trans) == 0)
                {
                    var (insertEntrySql, insertEntryParams) = genericUtil.BuildInsertEntrySql(sqlEntry);
                    await conn.ExecuteAsync(insertEntrySql, insertEntryParams, trans);
                }
                var deleteValuesSql = genericUtil.BuildDeleteValuesSql(MasterGenericRecordGroupIds.JobMetadata);
                await conn.ExecuteAsync(deleteValuesSql, new { RecordUniqueId = sqlEntry.RecordUniqueId }, trans);
                var (insertValuesSql, paramRows) = genericUtil.BuildInsertEntryValuesSql(sqlEntry);
                await conn.ExecuteAsync(insertValuesSql, paramRows, trans);
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

    private string BuildUpsertSql()
    {
        var t = TableName();
        var (cols, vals) = InsertColumnsAndParams();
        var conflictCols = $"{Col(x => x.ClusterId)}, {Col(x => x.Id)}";
        return $"INSERT INTO {t} ({cols}) VALUES ({vals}) ON CONFLICT ({conflictCols}) DO UPDATE SET {UpdateSetClause()} WHERE {t}.{Col(x => x.Version)} = @ExpectedVersion;";
    }

    protected override bool IsDupeViolation(Guid jobId, Exception ex)
    {
        return ex is PostgresException pgEx && pgEx.SqlState == "23505";
    }
}
