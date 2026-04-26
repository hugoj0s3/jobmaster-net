using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.MySql.Master;

internal class MySqlMasterRecurringSchedulesRepository : SqlMasterRecurringSchedulesRepository
{
    public MySqlMasterRecurringSchedulesRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;

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

            var unlockedGuard =
                $"(s.{Col(x => x.PartitionLockId)} IS NULL OR s.{Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
            var defaultOrderByClause = $" ORDER BY s.{cLastPlanCoverageUntil} DESC";
            var orderBy = SqlOrderByUtil.BuildOrderByClause(queryCriteria.SortBy, "s", defaultOrderByClause);

            var offsetClause = string.Empty;
            if (queryCriteria.CountLimit > 0)
            {
                offsetClause = $"\nLIMIT {queryCriteria.CountLimit}";
            }

            var selectCols = SelectProjection("s", "e", "v");

            var args = new Dictionary<string, object?>();
            foreach (var kv in whereArgs) args[kv.Key] = kv.Value;
            args["ClusterId"] = ClusterConnConfig.ClusterId;
            args["LockNowUtc"] = nowUtcWithSkew;
            args["PartitionLockId"] = partitionLockId;
            args["LockExpiresAt"] = expiresAtUtcKind;
            args["GroupId"] = MasterGenericRecordGroupIds.RecurringScheduleMetadata;

            // Step 1: atomically claim rows via UPDATE ... ORDER BY ... LIMIT
            var updateSql = $@"
UPDATE {t} s
SET s.{Col(x => x.PartitionLockId)} = @PartitionLockId,
    s.{Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    s.{Col(x => x.Version)} = {sql.GenerateVersionSql()}
{whereSql} AND {unlockedGuard}
{orderBy}
{offsetClause};";

            var rowsAffected = await conn.ExecuteAsync(updateSql, args, tx);

            if (rowsAffected == 0)
            {
                tx.Commit();
                return new List<RecurringScheduleRawModel>();
            }

            // Step 2: fetch claimed rows with metadata
            var fetchSql = $@"
SELECT {selectCols}
FROM {t} s
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = s.{cId} AND e.{Col(x => x.GroupId)} = @GroupId
LEFT JOIN {genericUtil.EntryValueTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata)} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)}
WHERE s.{cClusterId} = @ClusterId
  AND s.{Col(x => x.PartitionLockId)} = @PartitionLockId
  AND s.{Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt
{orderBy};";

            var linearRows = (await conn.QueryAsync<RecurringSchedulePersistenceRecordLinearDto>(fetchSql, args, tx)).ToList();
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
            var rowsAffected = conn.Execute(BuildScheduleUpsertSql(), dp, trans);

            // MySQL ON DUPLICATE KEY UPDATE rowsAffected:
            // 1 = inserted, 2 = updated, 0 = IF() prevented version update (conflict)
            if (rowsAffected != 1)
            {
                var t = TableName();
                var currentVersion = conn.ExecuteScalar<string>(
                    $"SELECT {Col(x => x.Version)} FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id",
                    new { rec.ClusterId, rec.Id }, trans);
                if (currentVersion != rec.Version)
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
            var rowsAffected = await conn.ExecuteAsync(BuildScheduleUpsertSql(), dp, trans);

            // MySQL ON DUPLICATE KEY UPDATE rowsAffected:
            // 1 = inserted, 2 = updated, 0 = IF() prevented version update (conflict)
            if (rowsAffected != 1)
            {
                var t = TableName();
                var currentVersion = await conn.ExecuteScalarAsync<string>(
                    $"SELECT {Col(x => x.Version)} FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id",
                    new { rec.ClusterId, rec.Id }, trans);
                if (currentVersion != rec.Version)
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

    private string BuildScheduleUpsertSql()
    {
        var t = TableName();
        var (cols, vals) = InsertColumnsAndParams();
        var cVersion = Col(x => x.Version);
        return $@"
INSERT INTO {t} ({cols}) VALUES ({vals})
ON DUPLICATE KEY UPDATE
    {UpdateSetClauseWithoutVersion()},
    {cVersion} = IF({cVersion} = @ExpectedVersion OR (@ExpectedVersion IS NULL AND {cVersion} IS NULL), @Version, {cVersion});";
    }

    private string BuildMetadataEntryUpsertSql()
    {
        var t2 = genericUtil.EntryTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata);
        var cIsReady = genericUtil.ColSqlEntry(x => x.IsReady);
        return $@"
INSERT INTO {t2} (record_unique_id, cluster_id, group_id, entry_id, entry_id_guid, subject_type, subject_id, created_at, expires_at, {cIsReady})
VALUES (@RecordUniqueId, @ClusterId, @GroupId, @EntryId, @EntryIdGuid, @SubjectType, @SubjectId, @CreatedAt, @ExpiresAt, 0)
ON DUPLICATE KEY UPDATE
    subject_type = VALUES(subject_type),
    subject_id = VALUES(subject_id),
    expires_at = VALUES(expires_at);";
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
INSERT INTO {vt} ({cRecordId}, {cKeyName}, {cValueText}, {cValueBinary}, {cValueInt64}, {cValueBool}, {cValueDecimal}, {cValueDateTime}, {cValueGuid})
VALUES (@RecordUniqueId, @KeyName, @ValueText, @ValueBinary, @ValueInt64, @ValueBoolean, @ValueDecimal, @ValueDateTime, @ValueGuid)
ON DUPLICATE KEY UPDATE
    {cValueText} = VALUES({cValueText}),
    {cValueBinary} = VALUES({cValueBinary}),
    {cValueInt64} = VALUES({cValueInt64}),
    {cValueBool} = VALUES({cValueBool}),
    {cValueDecimal} = VALUES({cValueDecimal}),
    {cValueDateTime} = VALUES({cValueDateTime}),
    {cValueGuid} = VALUES({cValueGuid});";
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
}