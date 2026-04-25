using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Dapper;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.MySql.Master;

internal class MySqlMasterJobsRepository : SqlMasterJobsRepository
{
    public MySqlMasterJobsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager,
        IKnownExceptionIdentifier knownExceptionIdentifier) : base(clusterConnectionConfig, connectionManager, knownExceptionIdentifier)
    {
    }

    public override string MasterRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;

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

            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = conn.Execute(BuildJobUpsertSql(), dp, trans);

            // MySQL ON DUPLICATE KEY UPDATE rowsAffected:
            // 1 = inserted (new job, no conflict possible)
            // 2 = updated (version matched; all columns and Version were rewritten)
            // 0 = version mismatch: every column is IF()-guarded, so nothing changed (true no-op)
            if (rowsAffected != 1)
            {
                var t = TableName();
                var currentVersion = conn.ExecuteScalar<string>(
                    $"SELECT {Col(x => x.Version)} FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id",
                    new { rec.ClusterId, rec.Id }, trans);

                if (currentVersion != rec.Version)
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

            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = await conn.ExecuteAsync(BuildJobUpsertSql(), dp, trans);

            // MySQL ON DUPLICATE KEY UPDATE rowsAffected:
            // 1 = inserted (new job, no conflict possible)
            // 2 = updated (version matched; all columns and Version were rewritten)
            // 0 = version mismatch: every column is IF()-guarded, so nothing changed (true no-op)
            if (rowsAffected != 1)
            {
                var t = TableName();
                var currentVersion = await conn.ExecuteScalarAsync<string>(
                    $"SELECT {Col(x => x.Version)} FROM {t} WHERE {Col(x => x.ClusterId)} = @ClusterId AND {Col(x => x.Id)} = @Id",
                    new { rec.ClusterId, rec.Id }, trans);

                if (currentVersion != rec.Version)
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
    
    protected override string BuildQueryIdsSql(
        string whereSql,
        bool needsMetadataJoin,
        int countLimit,
        int offset,
        SortByCriteria? sortByCriteria)
    {
        var baseSql = base.BuildQueryIdsSql(whereSql, needsMetadataJoin, countLimit, offset, sortByCriteria);

        // MySQL doesn't support LIMIT inside IN subqueries directly.
        // Wrapping in a derived table is the standard workaround.
        // FOR UPDATE SKIP LOCKED inside the derived table gives the same race-free
        // candidate selection Postgres gets. Without it, concurrent workers can
        // SELECT the same candidate IDs and race on the UPDATE, leaving losers
        // with a silently smaller result set and causing downstream version
        // conflicts in fallback bucket assignment. Requires MySQL 8.0.1+.
        return $"SELECT {Col(x => x.Id)} FROM ({baseSql} FOR UPDATE SKIP LOCKED) AS _candidates";
    }

    private string BuildMetadataEntryUpsertSql()
    {
        var t2 = genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata);
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
ON DUPLICATE KEY UPDATE
    {cValueText} = VALUES({cValueText}),
    {cValueBinary} = VALUES({cValueBinary}),
    {cValueInt64} = VALUES({cValueInt64}),
    {cValueBool} = VALUES({cValueBool}),
    {cValueDecimal} = VALUES({cValueDecimal}),
    {cValueDateTime} = VALUES({cValueDateTime}),
    {cValueGuid} = VALUES({cValueGuid});";
    }

    private string BuildJobUpsertSql()
    {
        var t = TableName();
        var (cols, vals) = InsertColumnsAndParams();
        var cVersion = Col(x => x.Version);

        // MySQL note:
        // UpdateSetClauseWithoutVersion() writes every non-version column unconditionally
        // (just "col = @col"). If we only guard the Version column with IF(), a conflicting
        // upsert still overwrites Status, BucketId, PartitionLockId, etc. with stale values
        // while leaving Version untouched - and rowsAffected ends up 2 (not 0), because
        // those unguarded columns did change. That makes the conflict partially persist:
        // e.g. Status=InBucket + BucketId=<fallback> get written even though the caller
        // then throws JobMasterVersionConflictException and never pushes the job to the
        // fallback onboarding source. The job becomes orphaned in the fallback bucket.
        //
        // Fix: wrap every column assignment in the same IF() guard. On a version mismatch
        // no column changes, rowsAffected = 0, the check block below catches it and we
        // throw cleanly with the DB row fully unmodified.
        var versionMatch = $"({cVersion} = @ExpectedVersion OR (@ExpectedVersion IS NULL AND {cVersion} IS NULL))";
        var guardedSetClause = BuildGuardedUpdateSetClauseWithoutVersion(versionMatch);

        return $@"
INSERT INTO {t} ({cols}) VALUES ({vals})
ON DUPLICATE KEY UPDATE
    {guardedSetClause},
    {cVersion} = IF({versionMatch}, @Version, {cVersion});";
    }

    /// <summary>
    /// Wraps each "col = @param" pair returned by <see cref="SqlMasterJobsRepository.UpdateSetClauseWithoutVersion"/>
    /// in <c>IF(versionMatch, @param, col)</c> so the upsert becomes atomic w.r.t. the optimistic
    /// concurrency check: either every column is written (including Version) or none is.
    /// </summary>
    private string BuildGuardedUpdateSetClauseWithoutVersion(string versionMatch)
    {
        var baseClause = UpdateSetClauseWithoutVersion();
        var parts = baseClause.Split(new[] { ", " }, StringSplitOptions.None);
        var guarded = parts.Select(p =>
        {
            var eqIdx = p.IndexOf(" = ", StringComparison.Ordinal);
            if (eqIdx < 0)
                throw new InvalidOperationException(
                    $"Unexpected UpdateSetClauseWithoutVersion entry format: '{p}'. Expected 'col = @param'.");
            var col = p.Substring(0, eqIdx);
            var val = p.Substring(eqIdx + 3);
            return $"{col} = IF({versionMatch}, {val}, {col})";
        });
        return string.Join(", ", guarded);
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