using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;

namespace JobMaster.MySql.Master;

internal class MySqlMasterGenericRecordRepository : SqlMasterGenericRecordRepository
{
    public MySqlMasterGenericRecordRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;

    public override void Upsert(GenericRecordEntry recordEntry)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var sqlEntry = MapToSqlEntry(recordEntry);
            
            // MySQL-specific: Upsert entry using INSERT ... ON DUPLICATE KEY UPDATE
            var t = genericUtil.EntryTable(recordEntry.GroupId);
            var entryUpsertSql = $@"
INSERT INTO {t} (record_unique_id, cluster_id, group_id, entry_id, entry_id_guid, created_at, expires_at)
VALUES (@RecordUniqueId, @ClusterId, @GroupId, @EntryId, @EntryIdGuid, @CreatedAt, @ExpiresAt)
ON DUPLICATE KEY UPDATE
    expires_at = VALUES(expires_at);";
            entryUpsertSql = AppendSqlTag(entryUpsertSql, "Upsert.Entry", recordEntry.GroupId);

            var entryArgs = new Dictionary<string, object?>
            {
                {"RecordUniqueId", sqlEntry.RecordUniqueId},
                {"ClusterId", sqlEntry.ClusterId},
                {"GroupId", sqlEntry.GroupId},
                {"EntryId", sqlEntry.EntryId},
                {"EntryIdGuid", sqlEntry.EntryIdGuid},
                {"CreatedAt", sqlEntry.CreatedAt},
                {"ExpiresAt", sqlEntry.ExpiresAt}
            };

            conn.Execute(entryUpsertSql, entryArgs, transaction);

            // MySQL-specific: Upsert values using INSERT ... ON DUPLICATE KEY UPDATE (more efficient than delete-reinsert)
            if (sqlEntry.Values.Count > 0)
            {
                var vt = genericUtil.EntryValueTable(recordEntry.GroupId);
                
                // Use proper column names from SQL generator
                var cRecordId = genericUtil.ColVal(x => x.RecordUniqueId);
                var cKeyName = genericUtil.ColVal(x => x.KeyName);
                var cValueText = genericUtil.ColVal(x => x.ValueText);
                var cValueBinary = genericUtil.ColVal(x => x.ValueBinary);
                var cValueInt64 = genericUtil.ColVal(x => x.ValueInt64);
                var cValueBool = genericUtil.ColVal(x => x.ValueBool);
                var cValueDecimal = genericUtil.ColVal(x => x.ValueDecimal);
                var cValueDateTime = genericUtil.ColVal(x => x.ValueDateTime);
                var cValueGuid = genericUtil.ColVal(x => x.ValueGuid);
                
                var valueUpsertSql = $@"
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
                valueUpsertSql = AppendSqlTag(valueUpsertSql, "Upsert.Values", recordEntry.GroupId);
                
                var valueRows = sqlEntry.Values.Select(v => new
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
                
                conn.Execute(valueUpsertSql, valueRows, transaction);
            }

            transaction.Commit();
        }
        catch
        {
            transaction.SafeRollback();
            throw;
        }
    }

    public override async Task UpsertAsync(GenericRecordEntry recordEntry)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var sqlEntry = MapToSqlEntry(recordEntry);
            
            // MySQL-specific: Upsert entry using INSERT ... ON DUPLICATE KEY UPDATE
            var t = genericUtil.EntryTable(recordEntry.GroupId);
            var entryUpsertSql = $@"
INSERT INTO {t} (record_unique_id, cluster_id, group_id, entry_id, entry_id_guid, created_at, expires_at)
VALUES (@RecordUniqueId, @ClusterId, @GroupId, @EntryId, @EntryIdGuid, @CreatedAt, @ExpiresAt)
ON DUPLICATE KEY UPDATE
    expires_at = VALUES(expires_at);";
            entryUpsertSql = AppendSqlTag(entryUpsertSql, "UpsertAsync.Entry", recordEntry.GroupId);

            var entryArgs = new Dictionary<string, object?>
            {
                {"RecordUniqueId", sqlEntry.RecordUniqueId},
                {"ClusterId", sqlEntry.ClusterId},
                {"GroupId", sqlEntry.GroupId},
                {"EntryId", sqlEntry.EntryId},
                {"EntryIdGuid", sqlEntry.EntryIdGuid},
                {"CreatedAt", sqlEntry.CreatedAt},
                {"ExpiresAt", sqlEntry.ExpiresAt}
            };

            await conn.ExecuteAsync(entryUpsertSql, entryArgs, transaction);

            // MySQL-specific: Upsert values using INSERT ... ON DUPLICATE KEY UPDATE (more efficient than delete-reinsert)
            if (sqlEntry.Values.Count > 0)
            {
                var vt = genericUtil.EntryValueTable(recordEntry.GroupId);
                
                // Use proper column names from SQL generator
                var cRecordId = genericUtil.ColVal(x => x.RecordUniqueId);
                var cKeyName = genericUtil.ColVal(x => x.KeyName);
                var cValueText = genericUtil.ColVal(x => x.ValueText);
                var cValueBinary = genericUtil.ColVal(x => x.ValueBinary);
                var cValueInt64 = genericUtil.ColVal(x => x.ValueInt64);
                var cValueBool = genericUtil.ColVal(x => x.ValueBool);
                var cValueDecimal = genericUtil.ColVal(x => x.ValueDecimal);
                var cValueDateTime = genericUtil.ColVal(x => x.ValueDateTime);
                var cValueGuid = genericUtil.ColVal(x => x.ValueGuid);
                
                var valueUpsertSql = $@"
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
                valueUpsertSql = AppendSqlTag(valueUpsertSql, "UpsertAsync.Values", recordEntry.GroupId);
                
                var valueRows = sqlEntry.Values.Select(v => new
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
                
                await conn.ExecuteAsync(valueUpsertSql, valueRows, transaction);
            }

            transaction.Commit();
        }
        catch
        {
            transaction.SafeRollback();
            throw;
        }
    }
}
