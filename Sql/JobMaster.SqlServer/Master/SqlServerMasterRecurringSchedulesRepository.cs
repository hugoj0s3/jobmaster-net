using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.SqlBase;
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
        Guid partitionLockId,
        DateTime expiresAtUtc)
    {
        if (partitionLockId == Guid.Empty) throw new ArgumentException("partitionLockId must be a valid GUID", nameof(partitionLockId));
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
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = s.{cId} AND e.{Col(x => x.GroupId)} = @GroupId
LEFT JOIN {genericUtil.EntryValueTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata)} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)}
{orderBy};";

            var args = new Dictionary<string, object?>();
            foreach (var kv in whereArgs) args[kv.Key] = kv.Value;
            args["ClusterId"] = ClusterConnConfig.ClusterId;
            args["PartitionLockId"] = partitionLockId;
            args["LockExpiresAt"] = expiresAtUtcKind;
            args["LockNowUtc"] = nowUtcWithSkew;
            args["GroupId"] = MasterGenericRecordGroupIds.RecurringScheduleMetadata;

            var linearRows = (await conn.QueryAsync<RecurringSchedulePersistenceRecordLinearDto>(sqlText, args, tx)).ToList();
            var records = LinearListToDomain(linearRows);
            var result = records.Select(RecurringScheduleRawModel.RecoverFromDb).ToList();

            tx.Commit();
            return result;
        }
        catch
        {
            tx.SafeRollback();
            throw;
        }
    }

    public override void Upsert(RecurringScheduleRawModel scheduleRaw)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var rec = RecurringScheduleRawModel.ToPersistence(scheduleRaw);
            var expectedVersion = rec.Version;
            rec.Version = Guid.NewGuid().ToString("N").ToLowerInvariant();

            if (rec.Metadata is not null)
            {
                var sqlEntry = genericUtil.MapToSqlEntry(rec.Metadata);
                var entryArgs = BuildMetadataEntryArgs(sqlEntry);
                conn.Execute(BuildMetadataEntryUpsertSql(), entryArgs, trans);

                if (sqlEntry.Values.Count > 0)
                {
                    var valueRows = BuildMetadataValueRows(sqlEntry);
                    conn.Execute(BuildMetadataValuesUpsertSql(), valueRows, trans);
                }

                conn.Execute(genericUtil.BuildSetReadySql(MasterGenericRecordGroupIds.RecurringScheduleMetadata),
                    new { RecordUniqueId = sqlEntry.RecordUniqueId }, trans);
            }

            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = conn.Execute(BuildMergeSql(), dp, trans);

            if (rowsAffected == 0)
            {
                var t = TableName();
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

    public override async Task UpsertAsync(RecurringScheduleRawModel scheduleRaw)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var rec = RecurringScheduleRawModel.ToPersistence(scheduleRaw);
            var expectedVersion = rec.Version;
            rec.Version = Guid.NewGuid().ToString("N").ToLowerInvariant();

            if (rec.Metadata is not null)
            {
                var sqlEntry = genericUtil.MapToSqlEntry(rec.Metadata);
                var entryArgs = BuildMetadataEntryArgs(sqlEntry);
                await conn.ExecuteAsync(BuildMetadataEntryUpsertSql(), entryArgs, trans);

                if (sqlEntry.Values.Count > 0)
                {
                    var valueRows = BuildMetadataValueRows(sqlEntry);
                    await conn.ExecuteAsync(BuildMetadataValuesUpsertSql(), valueRows, trans);
                }

                await conn.ExecuteAsync(genericUtil.BuildSetReadySql(MasterGenericRecordGroupIds.RecurringScheduleMetadata),
                    new { RecordUniqueId = sqlEntry.RecordUniqueId }, trans);
            }

            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = await conn.ExecuteAsync(BuildMergeSql(), dp, trans);

            if (rowsAffected == 0)
            {
                var t = TableName();
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

    private string BuildMetadataEntryUpsertSql()
    {
        var t2 = genericUtil.EntryTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata);
        var cIsReady = genericUtil.ColSqlEntry(x => x.IsReady);
        return $@"
MERGE {t2} WITH (HOLDLOCK) AS target
USING (SELECT @RecordUniqueId AS record_unique_id) AS source
ON target.record_unique_id = source.record_unique_id
WHEN MATCHED THEN
    UPDATE SET
        subject_type = @SubjectType,
        subject_id = @SubjectId,
        expires_at = @ExpiresAt
WHEN NOT MATCHED THEN
    INSERT (record_unique_id, cluster_id, group_id, entry_id, entry_id_guid, subject_type, subject_id, created_at, expires_at, {cIsReady})
    VALUES (@RecordUniqueId, @ClusterId, @GroupId, @EntryId, @EntryIdGuid, @SubjectType, @SubjectId, @CreatedAt, @ExpiresAt, 0);";
    }

    private string BuildMetadataValuesUpsertSql()
    {
        var vt = genericUtil.EntryValueTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata);
        var cRecordId = genericUtil.ColVal(x => x.RecordUniqueId);
        var cKeyName = genericUtil.ColVal(x => x.KeyName);
        var cValueText = genericUtil.ColVal(x => x.ValueText);
        var cValueBinary = genericUtil.ColVal(x => x.ValueBinary);
        var cValueInt64 = genericUtil.ColVal(x => x.ValueInt64);
        var cValueBool = genericUtil.ColVal(x => x.ValueBool);
        var cValueDecimal = genericUtil.ColVal(x => x.ValueDecimal);
        var cValueDateTime = genericUtil.ColVal(x => x.ValueDateTime);
        var cValueGuid = genericUtil.ColVal(x => x.ValueGuid);

        return $@"
MERGE {vt} WITH (HOLDLOCK) AS target
USING (SELECT @RecordUniqueId AS {cRecordId}, @KeyName AS {cKeyName}) AS source
ON target.{cRecordId} = source.{cRecordId} AND target.{cKeyName} = source.{cKeyName}
WHEN MATCHED THEN
    UPDATE SET
        {cValueText} = @ValueText,
        {cValueBinary} = @ValueBinary,
        {cValueInt64} = @ValueInt64,
        {cValueBool} = @ValueBoolean,
        {cValueDecimal} = @ValueDecimal,
        {cValueDateTime} = @ValueDateTime,
        {cValueGuid} = @ValueGuid
WHEN NOT MATCHED THEN
    INSERT ({cRecordId}, {cKeyName}, {cValueText}, {cValueBinary}, {cValueInt64}, {cValueBool}, {cValueDecimal}, {cValueDateTime}, {cValueGuid})
    VALUES (@RecordUniqueId, @KeyName, @ValueText, @ValueBinary, @ValueInt64, @ValueBoolean, @ValueDecimal, @ValueDateTime, @ValueGuid);";
    }

    private static Dictionary<string, object?> BuildMetadataEntryArgs(SqlGenericRecordEntry sqlEntry)
    {
        return new Dictionary<string, object?>
        {
            { "RecordUniqueId", sqlEntry.RecordUniqueId },
            { "ClusterId", sqlEntry.ClusterId },
            { "GroupId", sqlEntry.GroupId },
            { "EntryId", sqlEntry.EntryId },
            { "EntryIdGuid", sqlEntry.EntryIdGuid },
            { "SubjectType", sqlEntry.SubjectType },
            { "SubjectId", sqlEntry.SubjectId },
            { "CreatedAt", sqlEntry.CreatedAt },
            { "ExpiresAt", sqlEntry.ExpiresAt }
        };
    }

    private static IEnumerable<object> BuildMetadataValueRows(SqlGenericRecordEntry sqlEntry)
    {
        return sqlEntry.Values.Select(v => new
        {
            RecordUniqueId = sqlEntry.RecordUniqueId,
            KeyName = v.KeyName,
            ValueText = v.ValueText,
            ValueBinary = v.ValueBinary,
            ValueInt64 = v.ValueInt64,
            ValueBoolean = v.ValueBool,
            ValueDecimal = v.ValueDecimal,
            ValueDateTime = v.ValueDateTime,
            ValueGuid = v.ValueGuid
        });
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
