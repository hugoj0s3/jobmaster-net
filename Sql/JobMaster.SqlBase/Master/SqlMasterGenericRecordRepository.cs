using System.Data;
using System.Linq.Expressions;
using System.Text;
using Dapper;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.SqlBase.Master;

public abstract class SqlMasterGenericRecordRepository : JobMasterClusterAwareRepository, IMasterGenericRecordRepository
{
    private ISqlGenerator sql = null!;
    private string connString = null!;
    private JobMasterConfigDictionary additionalConnConfig = null!;
    private IDbConnectionManager connManager = null!;
    private GenericRecordSqlUtil genericUtil = null!;
    
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

        var (sqlText, args) = genericUtil.BuildGetSql(groupId, entryId, includeExpired);

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
        using var conn = connManager.Open(connString, additionalConnConfig);
        criteria ??= new GenericRecordQueryCriteria();
        (sqlText, args) = BuildQuerySql(groupId, criteria);
            
        var result = conn.Query<SqlGenericRecordEntryLinearDto>(
            sqlText,
            args
        );

        return LinearListToDomain(result);
    }

    public async Task<IList<GenericRecordEntry>> QueryAsync(string groupId, GenericRecordQueryCriteria? criteria = null)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        criteria ??= new GenericRecordQueryCriteria();

        var (sqlText, args) = BuildQuerySql(groupId, criteria);
        
        var result = await conn.QueryAsync<SqlGenericRecordEntryLinearDto>(
            sqlText,
            args
        );
        
        return LinearListToDomain(result);
    }

    public void Upsert(GenericRecordEntry recordEntry)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var sqlEntry = MapToSqlEntry(recordEntry);
            var (sqlText, args) = BuildUpdateEntrySql(sqlEntry);
            var rowsAffected = conn.Execute(sqlText, args, transaction);
            if (rowsAffected == 0)
            {
                (sqlText, args) = BuildInsertEntrySql(sqlEntry);
                conn.Execute(sqlText, args, transaction);
            }

            // Replace values: clear and insert fresh to reflect current payload
            conn.Execute(BuildDeleteValuesSql(), new { RecordUniqueId = sqlEntry.RecordUniqueId }, transaction);
            InsertEntryValues(conn, transaction, sqlEntry);

            transaction.Commit();
        }
        catch (Exception)
        {
            transaction.Rollback();
            throw;
        }
    }

    public async Task UpsertAsync(GenericRecordEntry recordEntry)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var sqlEntry = MapToSqlEntry(recordEntry);
            var (sqlText, args) = BuildUpdateEntrySql(sqlEntry);
            var rowsAffected = await conn.ExecuteAsync(sqlText, args, transaction);
            if (rowsAffected == 0)
            {
                (sqlText, args) = BuildInsertEntrySql(sqlEntry);
                await conn.ExecuteAsync(sqlText, args, transaction);
            }
            
            // Replace values: clear and insert fresh to reflect current payload
            await conn.ExecuteAsync(BuildDeleteValuesSql(), new { RecordUniqueId = sqlEntry.RecordUniqueId }, transaction);
            await InsertEntryValuesAsync(conn, transaction, sqlEntry);

            transaction.Commit();
        } 
        catch (Exception)
        {
            transaction.Rollback();
            throw;
        }
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
            transaction.Commit();
        }
        catch (Exception)
        {
            transaction.Rollback();
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
            transaction.Commit();
        }
        catch (Exception)
        {
            transaction.Rollback();
            throw;
        }
    }

    public void Update(GenericRecordEntry recordEntry)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var sqlEntry = MapToSqlEntry(recordEntry);
            var (sqlText, args) = BuildUpdateEntrySql(sqlEntry);
            
            conn.Execute(sqlText, args, transaction);
            // replace values
            conn.Execute(BuildDeleteValuesSql(), new { RecordUniqueId = sqlEntry.RecordUniqueId }, transaction);
            InsertEntryValues(conn, transaction, sqlEntry);
            
            transaction.Commit();
        }
        catch (Exception)
        {
            transaction.Rollback();
            throw;
        }
    }

    public async Task UpdateAsync(GenericRecordEntry recordEntry)
    {
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var sqlEntry = MapToSqlEntry(recordEntry);
            var (sqlText, args) = BuildUpdateEntrySql(sqlEntry);
            
            await conn.ExecuteAsync(sqlText, args, transaction);
            await conn.ExecuteAsync(BuildDeleteValuesSql(), new { RecordUniqueId = sqlEntry.RecordUniqueId }, transaction);
            await InsertEntryValuesAsync(conn, transaction, sqlEntry);
            
            transaction.Commit();
        }
        catch (Exception)
        {
            transaction.Rollback();
            throw;
        }

    }

    public void Delete(string groupId, string id)
    {
        using var conn = connManager.Open(connString, additionalConnConfig);
        using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        var recordUniqueId = GenericRecordEntry.UniqueId(ClusterConnConfig.ClusterId, groupId, id);
        try
        {
            conn.Execute(BuildDeleteValuesSql(), new { RecordUniqueId = recordUniqueId });
            conn.Execute(BuildDeleteEntrySql(), new { RecordUniqueId = recordUniqueId });
            
            transaction.Commit();
        }
        catch (Exception)
        {
            transaction.Rollback();
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
            await conn.ExecuteAsync(BuildDeleteValuesSql(), new { RecordUniqueId = recordUniqueId }, transaction);
            await conn.ExecuteAsync(BuildDeleteEntrySql(), new { RecordUniqueId = recordUniqueId }, transaction);
            transaction.Commit();
        }
        catch (Exception)
        {
            transaction.Rollback();
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
            var t = EntryTable();
            var cRecordId  = EntryTable() + "." + Col(x => x.RecordUniqueId);
            var cClusterId = Col(x => x.ClusterId);
            var cGroupId   = Col(x => x.GroupId);
            var cCreatedAt = Col(x => x.CreatedAt);

            var selectSql = new StringBuilder($@"SELECT {cRecordId}
FROM {t}
WHERE {cClusterId} = @ClusterId AND {cGroupId} = @GroupId AND {cCreatedAt} <= @CreatedAtTo
ORDER BY {cCreatedAt} ASC, {cRecordId} ASC");
            selectSql.AppendLine();
            selectSql.Append(sql.OffsetQueryFor(limit, 0));

            var ids = (await conn.QueryAsync<string>(selectSql.ToString(), new
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
            var vt = EntryValueTable();
            var cValRecordId = ColVal(x => x.RecordUniqueId);
            var delValuesSql = $"DELETE FROM {vt} WHERE {this.sql.InClauseFor(cValRecordId, "@RecordUniqueIds")}";
            await conn.ExecuteAsync(delValuesSql, new { RecordUniqueIds = ids }, tx);

            // Then delete entries
            var delEntriesSql = $"DELETE FROM {t} WHERE {this.sql.InClauseFor(cRecordId, "@RecordUniqueIds")}";
            await conn.ExecuteAsync(delEntriesSql, new { RecordUniqueIds = ids }, tx);

            tx.Commit();
            return ids.Count;
        }
        catch
        {
            tx.Rollback();
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
            // Batch size tuned to avoid parameter limits on SQL Server and large command texts in general
            for (int offset = 0; offset < records.Count; offset += MaxBatchSize)
            {
                var batch = records.Skip(offset).Take(Math.Min(MaxBatchSize, records.Count - offset)).ToList();

                // Map to SQL entries first
                var sqlEntries = batch.Select(MapToSqlEntry).ToList();

                // Build one multi-values INSERT for entries
                var t = EntryTable();
                var cols = $@"{Col(x => x.RecordUniqueId)}, {Col(x => x.ClusterId)}, {Col(x => x.GroupId)}, {Col(x => x.EntryId)}, {ColSqlEntry(x => x.EntryIdGuid)}, {Col(x => x.SubjectType)}, {Col(x => x.SubjectId)}, {Col(x => x.CreatedAt)}, {Col(x => x.ExpiresAt)}";
                var sb = new StringBuilder($"INSERT INTO {t} ({cols}) VALUES \n");
                var dynParams = new DynamicParameters();

                for (int i = 0; i < sqlEntries.Count; i++)
                {
                    var e = sqlEntries[i];
                    sb.Append($"(@RecordUniqueId_{i}, @ClusterId_{i}, @GroupId_{i}, @EntryId_{i}, @EntryIdGuid_{i}, @SubjectType_{i}, @SubjectId_{i}, @CreatedAt_{i}, @ExpiresAt_{i})");
                    if (i < sqlEntries.Count - 1) sb.Append(",\n"); else sb.Append(";");

                    dynParams.Add($"RecordUniqueId_{i}", e.RecordUniqueId);
                    dynParams.Add($"ClusterId_{i}", e.ClusterId);
                    dynParams.Add($"GroupId_{i}", e.GroupId);
                    dynParams.Add($"EntryId_{i}", e.EntryId);
                    dynParams.Add($"EntryIdGuid_{i}", e.EntryIdGuid);
                    dynParams.Add($"SubjectType_{i}", e.SubjectType);
                    dynParams.Add($"SubjectId_{i}", e.SubjectId);
                    dynParams.Add($"CreatedAt_{i}", e.CreatedAt);
                    dynParams.Add($"ExpiresAt_{i}", e.ExpiresAt);
                }

                await conn.ExecuteAsync(sb.ToString(), dynParams, transaction);

                // Insert values for each entry (uses Dapper multi-exec under the hood per entry)
                foreach (var e in sqlEntries)
                {
                    await InsertEntryValuesAsync(conn, transaction, e);
                }
            }

            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }
    
    public async Task<int> DeleteExpiredAsync(DateTime expiresAtTo, int limit)
    {
        if (limit <= 0) throw new ArgumentException("Limit must be greater than 0");
        
        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var t = EntryTable();
            var cRecordId   = EntryTable() + "." + Col(x => x.RecordUniqueId);
            var cClusterId  = Col(x => x.ClusterId);
            var cExpiresAt  = Col(x => x.ExpiresAt);

            // Select ids to delete (scoped to cluster and expired), ordered and limited
            var selectSql = new StringBuilder($@"SELECT {cRecordId}
FROM {t}
WHERE {cClusterId} = @ClusterId AND {cExpiresAt} IS NOT NULL AND {cExpiresAt} <= @ExpiresAtTo
ORDER BY {cExpiresAt} ASC, {cRecordId} ASC");

            selectSql.AppendLine();
            selectSql.Append(sql.OffsetQueryFor(limit, 0));

            var ids = (await conn.QueryAsync<string>(selectSql.ToString(), new
            {
                ClusterId = ClusterConnConfig.ClusterId,
                ExpiresAtTo = DateTime.SpecifyKind(expiresAtTo, DateTimeKind.Utc)
            }, tx)).ToList();

            if (ids.Count == 0)
            {
                tx.Commit();
                return 0;
            }

            // Delete values first
            var vt = EntryValueTable();
            var cValRecordId = ColVal(x => x.RecordUniqueId);
            var delValuesSql = $"DELETE FROM {vt} WHERE {this.sql.InClauseFor(cValRecordId, "@RecordUniqueIds")}";
            await conn.ExecuteAsync(delValuesSql, new { RecordUniqueIds = ids }, tx);

            // Then delete entries
            var delEntriesSql = $"DELETE FROM {t} WHERE {this.sql.InClauseFor(cRecordId, "@RecordUniqueIds")}";
            await conn.ExecuteAsync(delEntriesSql, new { RecordUniqueIds = ids }, tx);

            tx.Commit();
            return ids.Count;
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }
    
    // Helpers
    private string EntryTable() => genericUtil.EntryTable();

    private string EntryValueTable() => genericUtil.EntryValueTable();

    private string Col(Expression<Func<GenericRecordEntry, object?>> prop) => genericUtil.Col(prop);
    
    private string ColVal(Expression<Func<SqlGenericRecordEntryValue, object?>> prop) => genericUtil.ColVal(prop);
    
    private string ColSqlEntry(Expression<Func<SqlGenericRecordEntry, object?>> prop) => genericUtil.ColSqlEntry(prop);
    
    
    public IList<GenericRecordEntry> LinearListToDomain(IEnumerable<SqlGenericRecordEntryLinearDto> result)
    {
        return genericUtil.LinearListToDomain(result);
    }
    
    private SqlGenericRecordEntry MapToSqlEntry(GenericRecordEntry src)
    {
        return genericUtil.MapToSqlEntry(src);
    }
    
    private (string Sql, object Args) BuildGetSql(string groupId, string entryId, bool includeExpired)
    {
        return genericUtil.BuildGetSql(groupId, entryId, includeExpired);
    }
    
    private (string Sql, object Args) BuildQuerySql(string groupId, GenericRecordQueryCriteria criteria)
    {
        return genericUtil.BuildQuerySql(groupId, criteria);
    }
    private (string, IDictionary<string, object?>) BuildUpdateEntrySql(SqlGenericRecordEntry entry)
    {
        return genericUtil.BuildUpdateEntrySql(entry);
    }

    private (string, IDictionary<string, object?>) BuildInsertEntrySql(SqlGenericRecordEntry entry)
    {
        return genericUtil.BuildInsertEntrySql(entry);
    }

    private void InsertEntryValues(IDbConnection conn, IDbTransaction tx, SqlGenericRecordEntry entry)
    {
        if (entry.Values.Count == 0) return;
        
        var (insertSql, rows) = genericUtil.BuildInsertEntryValuesSql(entry);

        conn.Execute(insertSql, rows, tx);
    }

    private async Task InsertEntryValuesAsync(IDbConnection conn, IDbTransaction tx, SqlGenericRecordEntry entry)
    {
        if (entry.Values.Count == 0) return;
        
        var (insertSql, rows) = genericUtil.BuildInsertEntryValuesSql(entry);

        await conn.ExecuteAsync(insertSql, rows, tx);
    }

    private string BuildDeleteValuesSql()
    {
        return genericUtil.BuildDeleteValuesSql();
    }

    private string BuildDeleteEntrySql()
    {
        return genericUtil.BuildDeleteEntrySql();
    }
}