using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Utils.Extensions;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.SqlServer.Master;

internal class SqlServerMasterRecurringSchedulesRepository : SqlMasterRecurringSchedulesRepository
{
    public SqlServerMasterRecurringSchedulesRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;

    public override async Task<IList<RecurringScheduleRawModel>> AcquireAndFetchAsync(
        RecurringScheduleQueryCriteria queryCriteria,
        int partitionLockId,
        DateTime expiresAtUtc)
    {
        if (partitionLockId <= 0) throw new ArgumentException("partitionLockId must be > 0", nameof(partitionLockId));
        if (queryCriteria == null) throw new ArgumentNullException(nameof(queryCriteria));

        var nowUtcWithSkew = JobMasterConstants.NowUtcWithSkewTolerance();
        var expiresAtUtcKind = DateTime.SpecifyKind(expiresAtUtc, DateTimeKind.Utc);

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig, ReadIsolationLevel.Consistent);
        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var (whereSql, whereArgs) = BuildWhere(queryCriteria);
            var t = TableName();

            var cId = Col(x => x.Id);
            var cClusterId = Col(x => x.ClusterId);
            var cLastPlanCoverageUntil = Col(x => x.LastPlanCoverageUntil);

            var unlockedGuard = $"(s.{Col(x => x.PartitionLockId)} IS NULL OR s.{Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
            var defaultOrderByClause = $" ORDER BY s.{cLastPlanCoverageUntil} DESC";
            var orderBy = SqlOrderByUtil.BuildOrderByClause(queryCriteria.SortBy, "s", defaultOrderByClause);

            var offsetClause = string.Empty;
            if (queryCriteria.CountLimit > 0)
            {
                offsetClause = "\n" + sql.OffsetQueryFor(queryCriteria.CountLimit, queryCriteria.Offset);
            }

            var selectCols = SelectProjection("s", "e", "v");

            var sqlText = $@"
DECLARE @lockedIds TABLE (id uniqueidentifier NOT NULL);

;WITH candidates AS (
    SELECT s.{cId} AS id
    FROM {t} s
    {whereSql} AND {unlockedGuard}
    {orderBy}
    {offsetClause}
)
UPDATE s
SET {Col(x => x.PartitionLockId)} = @PartitionLockId,
    {Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    {Col(x => x.Version)} = {sql.GenerateVersionSql()}
OUTPUT inserted.{cId} INTO @lockedIds(id)
FROM {t} s
JOIN candidates c ON c.id = s.{cId}
WHERE s.{cClusterId} = @ClusterId
  AND {unlockedGuard};

SELECT {selectCols}
FROM {t} s
JOIN @lockedIds l ON l.id = s.{cId}
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = s.{cId}
LEFT JOIN {genericUtil.EntryValueTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata)} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)}
{orderBy};";

            var args = new Dictionary<string, object?>();
            foreach (var kv in whereArgs) args[kv.Key] = kv.Value;
            args["ClusterId"] = ClusterConnConfig.ClusterId;
            args["PartitionLockId"] = partitionLockId;
            args["LockExpiresAt"] = expiresAtUtcKind;
            args["LockNowUtc"] = nowUtcWithSkew;

            var linearRows = (await conn.QueryAsync<RecurringSchedulePersistenceRecordLinearDto>(sqlText, args, tx)).ToList();
            var records = LinearListToDomain(linearRows);
            tx.Commit();
            return records.Select(RecurringScheduleRawModel.RecoverFromDb).ToList();
        }
        catch
        {
            tx.SafeDispose();
            throw;
        }
    }

    public override void Upsert(RecurringScheduleRawModel scheduleRaw)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var rec = RecurringScheduleRawModel.ToPersistence(scheduleRaw);
            var expectedVersion = rec.Version;
            rec.Version = Guid.NewGuid().ToString("N").ToLowerInvariant();

            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = conn.Execute(BuildMergeSql(), dp, trans);

            if (rowsAffected == 0)
            {
                var exists = conn.ExecuteScalar<bool>(
                    $"SELECT 1 FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id",
                    new { rec.ClusterId, rec.Id }, trans);
                if (exists)
                    throw new JobMasterVersionConflictException(scheduleRaw.Id, "RecurringSchedule", expectedVersion);
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
                
                var deleteValuesSql = genericUtil.BuildDeleteValuesSql(MasterGenericRecordGroupIds.RecurringScheduleMetadata);
                conn.Execute(deleteValuesSql, new { RecordUniqueId = sqlEntry.RecordUniqueId }, trans);
                var (insertValuesSql, paramRows) = genericUtil.BuildInsertEntryValuesSql(sqlEntry);
                conn.Execute(insertValuesSql, paramRows, trans);
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

    public override async Task UpsertAsync(RecurringScheduleRawModel scheduleRaw)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = TableName();
            var rec = RecurringScheduleRawModel.ToPersistence(scheduleRaw);
            var expectedVersion = rec.Version;
            rec.Version = Guid.NewGuid().ToString("N").ToLowerInvariant();

            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = await conn.ExecuteAsync(BuildMergeSql(), dp, trans);

            if (rowsAffected == 0)
            {
                var exists = await conn.ExecuteScalarAsync<bool>(
                    $"SELECT 1 FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id",
                    new { rec.ClusterId, rec.Id }, trans);
                if (exists)
                    throw new JobMasterVersionConflictException(scheduleRaw.Id, "RecurringSchedule", expectedVersion);
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
                var deleteValuesSql = genericUtil.BuildDeleteValuesSql(MasterGenericRecordGroupIds.RecurringScheduleMetadata);
                await conn.ExecuteAsync(deleteValuesSql, new { RecordUniqueId = sqlEntry.RecordUniqueId }, trans);
                var (insertValuesSql, paramRows) = genericUtil.BuildInsertEntryValuesSql(sqlEntry);
                await conn.ExecuteAsync(insertValuesSql, paramRows, trans);
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

    private string BuildMergeSql()
    {
        var t = TableName();
        var (cols, vals) = InsertColumnsAndParams();
        var cClusterId = Col(x => x.ClusterId);
        var cId = Col(x => x.Id);
        var cVersion = Col(x => x.Version);
        return $@";MERGE INTO {t} WITH (HOLDLOCK) AS target
USING (SELECT @ClusterId AS {cClusterId}, @Id AS {cId}) AS src
ON target.{cClusterId} = src.{cClusterId} AND target.{cId} = src.{cId}
WHEN MATCHED AND target.{cVersion} = @ExpectedVersion THEN UPDATE SET {UpdateSetClause()}
WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals});";
    }
}
