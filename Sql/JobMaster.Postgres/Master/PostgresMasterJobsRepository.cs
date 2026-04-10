using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Dapper;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.SqlBase.Connections;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
using JobMaster.SqlBase.Scripts;
using Npgsql;

namespace JobMaster.Postgres.Master;

internal class PostgresMasterJobsRepository : SqlMasterJobsRepository
{
    public PostgresMasterJobsRepository(JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) :
        base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;

    public override async Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria,
        int partitionLockId, DateTime expiresAtUtc)
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

            var unlockedGuard =
                $"(j.{Col(x => x.PartitionLockId)} IS NULL OR j.{Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
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
            var rec = JobRawModel.ToPersistence(jobRaw);
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

                conn.Execute(genericUtil.BuildSetReadySql(MasterGenericRecordGroupIds.JobMetadata),
                    new { RecordUniqueId = sqlEntry.RecordUniqueId }, trans);
            }

            var t = TableName();
            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = conn.Execute(BuildJobUpsertSql(), dp, trans);

            if (rowsAffected == 0)
            {
                var exists = conn.ExecuteScalar<bool>(
                    $"SELECT 1 FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id",
                    new { rec.ClusterId, rec.Id }, trans);
                if (exists)
                    throw new JobMasterVersionConflictException(jobRaw.Id, "Job", expectedVersion);
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
            var rec = JobRawModel.ToPersistence(jobRaw);
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

                await conn.ExecuteAsync(genericUtil.BuildSetReadySql(MasterGenericRecordGroupIds.JobMetadata),
                    new { RecordUniqueId = sqlEntry.RecordUniqueId }, trans);
            }

            var t = TableName();
            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = await conn.ExecuteAsync(BuildJobUpsertSql(), dp, trans);

            if (rowsAffected == 0)
            {
                var exists = await conn.ExecuteScalarAsync<bool>(
                    $"SELECT 1 FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id",
                    new { rec.ClusterId, rec.Id }, trans);
                if (exists)
                    throw new JobMasterVersionConflictException(jobRaw.Id, "Job", expectedVersion);
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
    
    protected override bool IsDupeViolation(Guid jobId, Exception ex)
    {
        return ex is PostgresException pgEx && pgEx.SqlState == "23505";
    }

    private string BuildJobUpsertSql()
    {
        var t = TableName();
        var (cols, vals) = InsertColumnsAndParams();
        var conflictCols = $"{Col(x => x.ClusterId)}, {Col(x => x.Id)}";
        var cVersion = Col(x => x.Version);
        return $@"
INSERT INTO {t} ({cols}) VALUES ({vals})
ON CONFLICT ({conflictCols}) DO UPDATE SET
    {UpdateSetClause()}
WHERE {t}.{cVersion} = @ExpectedVersion;";
    }

    private string BuildMetadataEntryUpsertSql()
    {
        var t2 = genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata);
        var cIsReady = genericUtil.ColSqlEntry(x => x.IsReady);
        return $@"
INSERT INTO {t2} (record_unique_id, cluster_id, group_id, entry_id, entry_id_guid, subject_type, subject_id, created_at, expires_at, {cIsReady})
VALUES (@RecordUniqueId, @ClusterId, @GroupId, @EntryId, @EntryIdGuid, @SubjectType, @SubjectId, @CreatedAt, @ExpiresAt, false)
ON CONFLICT (record_unique_id) DO UPDATE SET
    subject_type = EXCLUDED.subject_type,
    subject_id = EXCLUDED.subject_id,
    expires_at = EXCLUDED.expires_at;";
    }

    private string BuildMetadataValuesUpsertSql()
    {
        var vt = genericUtil.EntryValueTable(MasterGenericRecordGroupIds.JobMetadata);
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
ON CONFLICT ({cRecordId}, {cKeyName}) DO UPDATE SET
    {cValueText} = EXCLUDED.{cValueText},
    {cValueBinary} = EXCLUDED.{cValueBinary},
    {cValueInt64} = EXCLUDED.{cValueInt64},
    {cValueBool} = EXCLUDED.{cValueBool},
    {cValueDecimal} = EXCLUDED.{cValueDecimal},
    {cValueDateTime} = EXCLUDED.{cValueDateTime},
    {cValueGuid} = EXCLUDED.{cValueGuid};";
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