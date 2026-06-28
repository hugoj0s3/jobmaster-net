using System.Data;
using System.Linq.Expressions;
using System.Text;
using Dapper;
using JobMaster.SqlBase.Extensions;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.SqlBase.Master;

internal abstract class SqlMasterGenericRecordRepository : JobMasterClusterAwareRepository, IMasterGenericRecordRepository
{
    private ISqlGenerator sql = null!;
    protected string connString = null!;
    protected JobMasterConfigDictionary additionalConnConfig = null!;
    protected IDbConnectionManager connManager = null!;
    protected GenericRecordSqlUtil genericUtil = null!;
    
    protected SqlMasterGenericRecordRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connManager
    ) : base(clusterConnectionConfig)
    {
        this.connManager = connManager;
        sql = SqlGeneratorFactory.Get(this.MasterRepoTypeId);
        connString = clusterConnectionConfig.ConnectionString;   
        additionalConnConfig = clusterConnectionConfig.AdditionalConnConfig;
        genericUtil = new GenericRecordSqlUtil(sql, additionalConnConfig, ClusterConnConfig.ClusterId);
    }
    
    public GenericRecordEntry? Get(string groupId, string entryId, bool includeExpired = false)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);

        var (sqlText, args) = BuildGetSql(groupId, entryId, includeExpired);

        var result = conn.Query<SqlGenericRecordEntryLinearDto>(
            sqlText,
            args);

        return LinearListToDomain(result).FirstOrDefault();
    }

    public async Task<GenericRecordEntry?> GetAsync(string groupId, string entryId, bool includeExpired = false)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        
        var (sqlText, args) = BuildGetSql(groupId, entryId, includeExpired);

        var result = await conn.QueryAsync<SqlGenericRecordEntryLinearDto>(
            sqlText,
            args);

        return LinearListToDomain(result).FirstOrDefault();
    }

    public IList<GenericRecordEntry> Query(string groupId, GenericRecordQueryCriteria? criteria = null)
    {
        string sqlText;
        object args;
        
        criteria ??= new GenericRecordQueryCriteria();
        using var conn = connManager.Open(connString, additionalConnConfig);
        (sqlText, args) = BuildQuerySql(groupId, criteria);
            
        var result = conn.Query<SqlGenericRecordEntryLinearDto>(
            sqlText,
            args
        );

        return LinearListToDomain(result);
    }

    public async Task<IList<GenericRecordEntry>> QueryAsync(string groupId, GenericRecordQueryCriteria? criteria = null)
    {
        criteria ??= new GenericRecordQueryCriteria();
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);

        var (sqlText, args) = BuildQuerySql(groupId, criteria);
        
        var result = await conn.QueryAsync<SqlGenericRecordEntryLinearDto>(
            sqlText,
            args
        );
        
        return LinearListToDomain(result);
    }

    

    public void Insert(GenericRecordEntry recordEntry)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var sqlEntry = MapToSqlEntry(recordEntry);
            var (sqlText, args) = BuildInsertEntrySql(sqlEntry);
            conn.Execute(sqlText, args, transaction);
            InsertEntryValues(conn, transaction, sqlEntry);
            conn.Execute(genericUtil.BuildSetReadySql(recordEntry.GroupId), new { RecordUniqueId = sqlEntry.RecordUniqueId }, transaction);
            transaction.Commit();
        }
        catch (Exception)
        {
            transaction.SafeRollback();
            throw;
        }
    }
    
    public async Task InsertAsync(GenericRecordEntry recordEntry)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var sqlEntry = MapToSqlEntry(recordEntry);
            var (sqlText, args) = BuildInsertEntrySql(sqlEntry);
            await conn.ExecuteAsync(sqlText, args, transaction);
            await InsertEntryValuesAsync(conn, transaction, sqlEntry);
            await conn.ExecuteAsync(genericUtil.BuildSetReadySql(recordEntry.GroupId), new { RecordUniqueId = sqlEntry.RecordUniqueId }, transaction);
            transaction.Commit();
        }
        catch (Exception)
        {
            transaction.SafeRollback();
            throw;
        }
    }

    public abstract void Upsert(GenericRecordEntry recordEntry);
    public abstract Task UpsertAsync(GenericRecordEntry recordEntry);

    public void Delete(string groupId, string id)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        var recordUniqueId = GenericRecordEntry.UniqueId(ClusterConnConfig.ClusterId, groupId, id);
        try
        {
            conn.Execute(BuildDeleteValuesSql(groupId), new { RecordUniqueId = recordUniqueId });
            conn.Execute(BuildDeleteEntrySql(groupId), new { RecordUniqueId = recordUniqueId });
            
            transaction.Commit();
        }
        catch (Exception)
        {
            transaction.SafeRollback();
            throw;
        }
    }

    public async Task DeleteAsync(string groupId, string id)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        var recordUniqueId = GenericRecordEntry.UniqueId(ClusterConnConfig.ClusterId, groupId, id);
        try
        {
            await conn.ExecuteAsync(BuildDeleteValuesSql(groupId), new { RecordUniqueId = recordUniqueId }, transaction);
            await conn.ExecuteAsync(BuildDeleteEntrySql(groupId), new { RecordUniqueId = recordUniqueId }, transaction);
            transaction.Commit();
        }
        catch (Exception)
        {
            transaction.SafeRollback();
            throw;
        }
    }
    
    public async Task<int> DeleteByCreatedAtAsync(string groupId, DateTime createdAtTo, int limit)
    {
        if (string.IsNullOrEmpty(groupId)) throw new ArgumentException("groupId is required", nameof(groupId));
        if (limit <= 0) throw new ArgumentException("Limit must be greater than 0", nameof(limit));

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = genericUtil.EntryTable(groupId);
            var cRecordId  = t + "." + Col(x => x.RecordUniqueId);
            var cClusterId = Col(x => x.ClusterId);
            var cGroupId   = Col(x => x.GroupId);
            var cCreatedAt = Col(x => x.CreatedAt);

            var selectSql = new StringBuilder($@"SELECT {cRecordId}
FROM {t}
WHERE {cClusterId} = @ClusterId AND {cGroupId} = @GroupId AND {cCreatedAt} <= @CreatedAtTo
ORDER BY {cCreatedAt} ASC, {cRecordId} ASC");
            selectSql.AppendLine();
            selectSql.Append(sql.OffsetQueryFor(limit, 0));

            var ids = (await conn.QueryAsync<string>(AppendSqlTag(selectSql.ToString(), "DeleteByCreatedAt.SelectIds", groupId), new
            {
                ClusterId = ClusterConnConfig.ClusterId,
                GroupId = groupId,
                CreatedAtTo = DateTime.SpecifyKind(createdAtTo, DateTimeKind.Utc)
            }, tx)).ToList();

            if (ids.Count == 0)
            {
                tx.Commit();
                return 0;
            }

            // Delete values first
            var vt = genericUtil.EntryValueTable(groupId);
            var cValRecordId = ColVal(x => x.RecordUniqueId);
            var delValuesSql = AppendSqlTag($"DELETE FROM {vt} WHERE {this.sql.InClauseFor(cValRecordId, "@RecordUniqueIds")}", "DeleteByCreatedAt.DeleteValues", groupId);
            await conn.ExecuteAsync(delValuesSql, new { RecordUniqueIds = ids }, tx);

            // Then delete entries
            var delEntriesSql = AppendSqlTag($"DELETE FROM {t} WHERE {this.sql.InClauseFor(cRecordId, "@RecordUniqueIds")}", "DeleteByCreatedAt.DeleteEntries", groupId);
            await conn.ExecuteAsync(delEntriesSql, new { RecordUniqueIds = ids }, tx);

            tx.Commit();
            return ids.Count;
        }
        catch
        {
            tx.SafeRollback();
            throw;
        }
    }

    private const int MaxBatchSize = JobMasterConstants.MaxBatchSizeForBulkOperation;
    public virtual async Task BulkInsertAsync(IList<GenericRecordEntry> records)
    {
        if (records.Count == 0) return;

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            // Group records by family so each INSERT targets the correct table
            var byFamily = records.GroupBy(r => GenericRecordSqlUtil.ResolveFamilySuffix(r.GroupId));
            foreach (var familyGroup in byFamily)
            {
                var familyRecords = familyGroup.ToList();
                for (int offset = 0; offset < familyRecords.Count; offset += MaxBatchSize)
                {
                    var batch = familyRecords.Skip(offset).Take(Math.Min(MaxBatchSize, familyRecords.Count - offset)).ToList();

                    // Map to SQL entries first
                    var sqlEntries = batch.Select(MapToSqlEntry).ToList();

                    // Build one multi-values INSERT for entries
                    var t = genericUtil.EntryTable(batch[0].GroupId);
                    var cols = $@"{Col(x => x.RecordUniqueId)}, {Col(x => x.ClusterId)}, {Col(x => x.GroupId)}, {Col(x => x.EntryId)}, {ColSqlEntry(x => x.EntryIdGuid)}, {Col(x => x.CreatedAt)}, {Col(x => x.ExpiresAt)}, {ColSqlEntry(x => x.IsReady)}";
                    var sb = new StringBuilder($"INSERT INTO {t} ({cols}) VALUES \n");
                    var dynParams = new DynamicParameters();

                    for (int i = 0; i < sqlEntries.Count; i++)
                    {
                        var e = sqlEntries[i];
                        sb.Append($"(@RecordUniqueId_{i}, @ClusterId_{i}, @GroupId_{i}, @EntryId_{i}, @EntryIdGuid_{i}, @CreatedAt_{i}, @ExpiresAt_{i}, @IsReady_{i})");
                        if (i < sqlEntries.Count - 1) sb.Append(",\n"); else sb.Append(";");

                        dynParams.Add($"RecordUniqueId_{i}", e.RecordUniqueId);
                        dynParams.Add($"ClusterId_{i}", e.ClusterId);
                        dynParams.Add($"GroupId_{i}", e.GroupId);
                        dynParams.Add($"EntryId_{i}", e.EntryId);
                        dynParams.Add($"EntryIdGuid_{i}", e.EntryIdGuid);
                        dynParams.Add($"CreatedAt_{i}", e.CreatedAt);
                        dynParams.Add($"ExpiresAt_{i}", e.ExpiresAt);
                        dynParams.Add($"IsReady_{i}", false);
                    }

                    await conn.ExecuteAsync(AppendSqlTag(sb.ToString(), "BulkInsert.InsertEntries", batch[0].GroupId), dynParams, transaction);

                    // Insert values for each entry (uses Dapper multi-exec under the hood per entry)
                    foreach (var e in sqlEntries)
                    {
                        await InsertEntryValuesAsync(conn, transaction, e);
                    }

                    // Final step for batch: set IsReady = 1
                    var setReadySql = genericUtil.BuildSetReadyMultipleSql(batch[0].GroupId, "@Ids");
                    await conn.ExecuteAsync(AppendSqlTag(setReadySql, "BulkInsert.SetReady", batch[0].GroupId), new { Ids = sqlEntries.Select(x => x.RecordUniqueId).ToArray() }, transaction);
                }
            }

            transaction.Commit();
        }
        catch
        {
            transaction.SafeRollback();
            throw;
        }
    }
    
    public async Task<int> DeleteExpiredAsync(DateTime expiresAtTo, int limit)
    {
        if (limit <= 0) throw new ArgumentException("Limit must be greater than 0");
        
        var totalDeleted = 0;
        // Iterate over all family suffixes plus the default (empty) table
        var suffixes = new List<string> { string.Empty };
        suffixes.AddRange(GenericRecordSqlUtil.AllFamilySuffixes);

        foreach (var suffix in suffixes)
        {
            using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
            using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
            try
            {
                var t = genericUtil.EntryTableForSuffix(suffix);
                var cRecordId   = t + "." + Col(x => x.RecordUniqueId);
                var cClusterId  = Col(x => x.ClusterId);
                var cExpiresAt  = Col(x => x.ExpiresAt);

                var selectSql = new StringBuilder($@"SELECT {cRecordId}
FROM {t}
WHERE {cClusterId} = @ClusterId AND {cExpiresAt} IS NOT NULL AND {cExpiresAt} <= @ExpiresAtTo
ORDER BY {cExpiresAt} ASC, {cRecordId} ASC");

                selectSql.AppendLine();
                selectSql.Append(sql.OffsetQueryFor(limit, 0));

                var ids = (await conn.QueryAsync<string>(AppendSqlTag(selectSql.ToString(), "DeleteExpired.SelectIds", "*", $"familySuffix={suffix}"), new
                {
                    ClusterId = ClusterConnConfig.ClusterId,
                    ExpiresAtTo = DateTime.SpecifyKind(expiresAtTo, DateTimeKind.Utc)
                }, tx)).ToList();

                if (ids.Count == 0)
                {
                    tx.Commit();
                    continue;
                }

                // Delete values first
                var vt = genericUtil.EntryValueTableForSuffix(suffix);
                var cValRecordId = ColVal(x => x.RecordUniqueId);
                var delValuesSql = AppendSqlTag($"DELETE FROM {vt} WHERE {this.sql.InClauseFor(cValRecordId, "@RecordUniqueIds")}", "DeleteExpired.DeleteValues", "*", $"familySuffix={suffix}");
                await conn.ExecuteAsync(delValuesSql, new { RecordUniqueIds = ids }, tx);

                // Then delete entries
                var delEntriesSql = AppendSqlTag($"DELETE FROM {t} WHERE {this.sql.InClauseFor(cRecordId, "@RecordUniqueIds")}", "DeleteExpired.DeleteEntries", "*", $"familySuffix={suffix}");
                await conn.ExecuteAsync(delEntriesSql, new { RecordUniqueIds = ids }, tx);

                tx.Commit();
                totalDeleted += ids.Count;
            }
            catch
            {
                tx.SafeRollback();
                throw;
            }
        }

        return totalDeleted;
    }

    public int Count(string groupId, GenericRecordQueryCriteria? criteria = null)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        criteria ??= new GenericRecordQueryCriteria();

        var (sqlText, args) = genericUtil.BuildCountSql(groupId, criteria);
        return conn.ExecuteScalar<int>(AppendSqlTag(sqlText, "Count", groupId), args);
    }

    public async Task<int> CountAsync(string groupId, GenericRecordQueryCriteria? criteria = null)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        criteria ??= new GenericRecordQueryCriteria();

        var (sqlText, args) = genericUtil.BuildCountSql(groupId, criteria);
        return await conn.ExecuteScalarAsync<int>(AppendSqlTag(sqlText, "CountAsync", groupId), args);
    }

    private string Col(Expression<Func<GenericRecordEntry, object?>> prop) => genericUtil.Col(prop);
    
    private string ColVal(Expression<Func<SqlGenericRecordEntryValue, object?>> prop) => genericUtil.ColVal(prop);
    
    private string ColSqlEntry(Expression<Func<SqlGenericRecordEntry, object?>> prop) => genericUtil.ColSqlEntry(prop);
    
    
    private IList<GenericRecordEntry> LinearListToDomain(IEnumerable<SqlGenericRecordEntryLinearDto> result)
    {
        return genericUtil.LinearListToDomain(result);
    }
    
    protected SqlGenericRecordEntry MapToSqlEntry(GenericRecordEntry src)
    {
        return genericUtil.MapToSqlEntry(src);
    }
    
    private (string Sql, object Args) BuildGetSql(string groupId, string entryId, bool includeExpired)
    {
        var (sqlText, args) = genericUtil.BuildGetSql(groupId, entryId, includeExpired);
        sqlText = AppendSqlTag(sqlText, "Get", groupId);
        return (sqlText, args);
    }
    
    private (string Sql, object Args) BuildQuerySql(string groupId, GenericRecordQueryCriteria criteria)
    {
        var (sqlText, args) = genericUtil.BuildQuerySql(groupId, criteria);
        sqlText = AppendSqlTag(sqlText, "Query", groupId);
        return (sqlText, args);
    }
    
    private (string, IDictionary<string, object?>) BuildUpdateEntrySql(SqlGenericRecordEntry entry)
    {
        var (sqlText, args) = genericUtil.BuildUpdateEntrySql(entry);
        sqlText = AppendSqlTag(sqlText, "UpdateEntry", entry.GroupId);
        return (sqlText, args);
    }

    private (string, IDictionary<string, object?>) BuildInsertEntrySql(SqlGenericRecordEntry entry)
    {
        var (sqlText, args) = genericUtil.BuildInsertEntrySql(entry);
        sqlText = AppendSqlTag(sqlText, "InsertEntry", entry.GroupId);
        return (sqlText, args);
    }

    private void InsertEntryValues(IDbConnection conn, IDbTransaction tx, SqlGenericRecordEntry entry)
    {
        if (entry.Values.Count == 0) return;
        
        var (insertSql, rows) = genericUtil.BuildInsertEntryValuesSql(entry);

        conn.Execute(AppendSqlTag(insertSql, "InsertEntryValues", entry.GroupId), rows, tx);
    }

    private async Task InsertEntryValuesAsync(IDbConnection conn, IDbTransaction tx, SqlGenericRecordEntry entry)
    {
        if (entry.Values.Count == 0) return;
        
        var (insertSql, rows) = genericUtil.BuildInsertEntryValuesSql(entry);

        await conn.ExecuteAsync(AppendSqlTag(insertSql, "InsertEntryValuesAsync", entry.GroupId), rows, tx);
    }

    private string BuildDeleteValuesSql(string groupId)
    {
        return AppendSqlTag(genericUtil.BuildDeleteValuesSql(groupId), "DeleteValues", groupId);
    }

    private string BuildDeleteEntrySql(string groupId)
    {
        return AppendSqlTag(genericUtil.BuildDeleteEntrySql(groupId), "DeleteEntry", groupId);
    }
    
    protected string AppendSqlTag(string sqlText, string operation, string? groupId = null, string? scope = null)
    {
        var normalizedGroup = string.IsNullOrWhiteSpace(groupId) ? "*" : SanitizeTagValue(groupId!);
        var normalizedOp = SanitizeTagValue(operation);
        var normalizedScope = string.IsNullOrWhiteSpace(scope) ? null : SanitizeTagValue(scope!);
        var scopePart = normalizedScope is null ? string.Empty : $";scope={normalizedScope}";

        return $"/* JM:repo=GenericRecord;op={normalizedOp};group={normalizedGroup}{scopePart} */\n{sqlText}";
    }
    
    private static string SanitizeTagValue(string value)
    {
        return value
            .Replace("*/", "* /")
            .Replace('\r', ' ')
            .Replace('\n', ' ');
    }
}
