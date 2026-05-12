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
        var unlockedGuard = $"({Col(x => x.PartitionLockId)} IS NULL OR {Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig, ReadIsolationLevel.Consistent);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var (whereSql, args) = BuildWhere(queryCriteria);
            var needsMetadataJoin = queryCriteria.MetadataFilters is { Count: > 0 };

            // Step 1: SELECT candidate IDs with FOR UPDATE SKIP LOCKED.
            // Rows are locked at SELECT time — the subsequent UPDATE uses the explicit
            // ID list, avoiding a nested subquery that MySQL cannot execute under LIMIT.
            var selectIdsSql = BuildQueryIdsToLockSql(
                whereSql, needsMetadataJoin,
                queryCriteria.CountLimit, queryCriteria.Offset, queryCriteria.SortBy);

            var selectIdsArgs = new Dictionary<string, object?>(args);
            selectIdsArgs["LockNowUtc"] = nowUtcWithSkew;
            if (needsMetadataJoin)
                selectIdsArgs["GroupId"] = MasterGenericRecordGroupIds.RecurringScheduleMetadata;

            var ids = (await conn.QueryAsync<Guid>(selectIdsSql, selectIdsArgs, trans)).ToList();

            if (ids.Count == 0)
            {
                trans.Commit();
                return new List<RecurringScheduleRawModel>();
            }

            // Step 2: Point-UPDATE using the explicit ID list.
            // FOR UPDATE locks from step 1 guarantee no competing transaction can
            // modify these rows between the SELECT and this UPDATE.
            var t = TableName();
            var inClause = sql.InClauseFor(Col(x => x.Id), "@Ids");
            var updateSql = $@"
UPDATE {t}
SET {Col(x => x.PartitionLockId)} = @PartitionLockId,
    {Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    {Col(x => x.Version)} = {sql.GenerateVersionSql()}
WHERE {Col(x => x.ClusterId)} = @ClusterId
  AND {inClause}
  AND {unlockedGuard};";

            var updateArgs = new Dictionary<string, object?>
            {
                { "ClusterId", ClusterConnConfig.ClusterId },
                { "Ids", ids },
                { "PartitionLockId", partitionLockId },
                { "LockExpiresAt", expiresAtUtcKind },
                { "LockNowUtc", nowUtcWithSkew }
            };

            await conn.ExecuteAsync(updateSql, updateArgs, trans);

            // Step 3: Fetch full records on the same connection and transaction,
            // filtered by partitionLockId to confirm each row was actually locked.
            var schedules = await FetchSchedulesByPartitionLockAsync(ids, partitionLockId, nowUtcWithSkew, conn, trans);

            // Partial guard: fewer rows returned than locked means something unexpected
            // happened. Roll back rather than returning a short batch.
            if (schedules.Count != ids.Count)
            {
                trans.SafeRollback();
                return new List<RecurringScheduleRawModel>();
            }

            trans.Commit();
            return schedules;
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    private async Task<IList<RecurringScheduleRawModel>> FetchSchedulesByPartitionLockAsync(
        List<Guid> ids,
        Guid partitionLockId,
        DateTime nowUtcWithSkew,
        IDbConnection conn,
        IDbTransaction trans)
    {
        var selectCols = SelectProjection();
        var t = TableName();
        var inClause = sql.InClauseFor($"s.{Col(x => x.Id)}", "@Ids");

        var sqlText = $@"
SELECT {selectCols}
FROM {t} s
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata)} e
    ON e.{Col(x => x.EntryIdGuid)} = s.{Col(x => x.Id)}
    AND e.{Col(x => x.GroupId)} = @GroupId
LEFT JOIN {genericUtil.EntryValueTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata)} v
    ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)}
WHERE s.{Col(x => x.ClusterId)} = @ClusterId
  AND {inClause}
  AND s.{Col(x => x.PartitionLockId)} = @PartitionLockId
  AND s.{Col(x => x.PartitionLockExpiresAt)} > @NowUtcWithSkew";

        var fetchArgs = new Dictionary<string, object?>
        {
            { "GroupId", MasterGenericRecordGroupIds.RecurringScheduleMetadata },
            { "ClusterId", ClusterConnConfig.ClusterId },
            { "Ids", ids },
            { "PartitionLockId", partitionLockId },
            { "NowUtcWithSkew", nowUtcWithSkew }
        };

        var linearRows = (await conn.QueryAsync<RecurringSchedulePersistenceRecordLinearDto>(sqlText, fetchArgs, trans)).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(RecurringScheduleRawModel.RecoverFromDb).ToList();
    }

    protected override string BuildQueryIdsToLockSql(
        string whereSql,
        bool needsMetadataJoin,
        int countLimit,
        int offset,
        SortByCriteria? sortByCriteria)
    {
        // Push the unlocked guard into the inner WHERE so LIMIT applies only to
        // rows that are actually acquirable. Without this, already-locked rows
        // consume LIMIT slots and the batch comes back smaller than expected.
        // @LockNowUtc is provided by AcquireAndFetchAsync's selectIdsArgs.
        var unlockedFilter = $" AND ({Col(x => x.PartitionLockId)} IS NULL " +
                             $"OR {Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
        var whereWithUnlocked = whereSql + unlockedFilter;

        var baseSql = base.BuildQueryIdsToLockSql(whereWithUnlocked, needsMetadataJoin, countLimit, offset, sortByCriteria);

        // MySQL does not allow LIMIT inside an IN subquery directly.
        // Wrapping in a derived table is the standard workaround.
        // FOR UPDATE SKIP LOCKED inside the derived table gives race-free candidate
        // selection — competing workers skip already-locked rows at SELECT time.
        return $"SELECT {Col(x => x.Id)} FROM ({baseSql} FOR UPDATE SKIP LOCKED) AS _candidates";
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