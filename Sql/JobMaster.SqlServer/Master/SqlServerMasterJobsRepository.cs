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

namespace JobMaster.SqlServer.Master;

internal class SqlServerMasterJobsRepository : SqlMasterJobsRepository
{
    public SqlServerMasterJobsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager,
        IKnownExceptionIdentifier knownExceptionIdentifier) : base(clusterConnectionConfig, connectionManager, knownExceptionIdentifier)
    {
    }

    public override string MasterRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;

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

            if (rowsAffected == 0)
            {
                var t = TableName();
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

            var dp = new DynamicParameters(rec);
            dp.Add("ExpectedVersion", expectedVersion);
            var rowsAffected = await conn.ExecuteAsync(BuildJobUpsertSql(), dp, trans);

            if (rowsAffected == 0)
            {
                var t = TableName();
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

    protected override string UpdateToLockTableHint => "WITH (UPDLOCK, READPAST)";

    private string BuildMetadataEntryUpsertSql()
    {
        var t2 = genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata);
        var cIsReady = genericUtil.ColSqlEntry(x => x.IsReady);
        return $@"
UPDATE {t2} WITH (UPDLOCK, SERIALIZABLE)
SET subject_type = @SubjectType,
    subject_id = @SubjectId,
    expires_at = @ExpiresAt
WHERE record_unique_id = @RecordUniqueId;

IF @@ROWCOUNT = 0
BEGIN
    IF NOT EXISTS (SELECT 1 FROM {t2} WITH (UPDLOCK, SERIALIZABLE) WHERE record_unique_id = @RecordUniqueId)
    BEGIN
        INSERT INTO {t2} (record_unique_id, cluster_id, group_id, entry_id, entry_id_guid, subject_type, subject_id, created_at, expires_at, {cIsReady})
        VALUES (@RecordUniqueId, @ClusterId, @GroupId, @EntryId, @EntryIdGuid, @SubjectType, @SubjectId, @CreatedAt, @ExpiresAt, 0);
    END
END";
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
UPDATE {vt} WITH (UPDLOCK, SERIALIZABLE)
SET {cValueText} = @ValueText,
    {cValueBinary} = @ValueBinary,
    {cValueInt64} = @ValueInt64,
    {cValueBool} = @ValueBoolean,
    {cValueDecimal} = @ValueDecimal,
    {cValueDateTime} = @ValueDateTime,
    {cValueGuid} = @ValueGuid
WHERE {cRecordId} = @RecordUniqueId AND {cKeyName} = @KeyName;

IF @@ROWCOUNT = 0
BEGIN
    IF NOT EXISTS (SELECT 1 FROM {vt} WITH (UPDLOCK, SERIALIZABLE) WHERE {cRecordId} = @RecordUniqueId AND {cKeyName} = @KeyName)
    BEGIN
        INSERT INTO {vt} ({cRecordId}, {cKeyName}, {cValueText}, {cValueBinary}, {cValueInt64}, {cValueBool}, {cValueDecimal}, {cValueDateTime}, {cValueGuid})
        VALUES (@RecordUniqueId, @KeyName, @ValueText, @ValueBinary, @ValueInt64, @ValueBoolean, @ValueDecimal, @ValueDateTime, @ValueGuid);
    END
END";
    }

    private string BuildJobUpsertSql()
    {
        var t = TableName();
        var (cols, vals) = InsertColumnsAndParams();
        var cClusterId = Col(x => x.ClusterId);
        var cId = Col(x => x.Id);
        var cVersion = Col(x => x.Version);

        return $@"
UPDATE {t} WITH (UPDLOCK, SERIALIZABLE)
SET {UpdateSetClause()}
WHERE {cClusterId} = @ClusterId
  AND {cId} = @Id
  AND ({cVersion} = @ExpectedVersion OR (@ExpectedVersion IS NULL AND {cVersion} IS NULL));

IF @@ROWCOUNT = 0
BEGIN
    IF NOT EXISTS (SELECT 1 FROM {t} WITH (UPDLOCK, SERIALIZABLE) WHERE {cClusterId} = @ClusterId AND {cId} = @Id)
    BEGIN
        INSERT INTO {t} ({cols}) VALUES ({vals});
    END
END";
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