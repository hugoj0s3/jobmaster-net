using System.Data;
using System.Text;
using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.SqlBase.Master;

internal abstract class SqlMasterLogsRepository : JobMasterClusterAwareRepository, IMasterLogsRepository
{
    protected IDbConnectionManager connManager = null!;
    protected ISqlGenerator sql = null!;
    protected string connString = string.Empty;
    protected JobMasterConfigDictionary additionalConnConfig = null!;

    protected SqlMasterLogsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connManager) : base(clusterConnectionConfig)
    {
        this.connManager = connManager;
        sql = SqlGeneratorFactory.Get(this.MasterRepoTypeId);
        connString = clusterConnectionConfig.ConnectionString;
        additionalConnConfig = clusterConnectionConfig.AdditionalConnConfig;
    }

    private string TableName()
    {
        var prefix = sql.GetTablePrefix(additionalConnConfig);
        return $"{prefix}log";
    }

    public async Task BulkInsertAsync(IList<LogItem> items)
    {
        if (items.Count == 0) return;

        var t = TableName();
        const int maxBatch = 100;

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            for (int offset = 0; offset < items.Count; offset += maxBatch)
            {
                var count = Math.Min(maxBatch, items.Count - offset);
                var (sb, dynParams) = BuildBulkInsertSql(t, items, offset, count);
                await conn.ExecuteAsync(sb.ToString(), dynParams, tx);
            }
            tx.Commit();
        }
        catch
        {
            tx.SafeRollback();
            throw;
        }
    }

    public async Task<List<LogItem>> QueryAsync(LogItemQueryCriteria criteria)
    {
        var t = TableName();
        var (whereSql, args) = BuildWhere(criteria);
        var sqlText = $"SELECT * FROM {t} {whereSql} ORDER BY timestamp_utc DESC\n{sql.OffsetQueryFor(criteria.CountLimit, criteria.Offset)}";

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var rows = await conn.QueryAsync<LogPersistenceRecord>(sqlText, args);
        return rows.Select(ToLogItem).ToList();
    }

    public async Task<int> CountAsync(LogItemQueryCriteria criteria)
    {
        var t = TableName();
        var (whereSql, args) = BuildWhere(criteria);
        var sqlText = $"SELECT COUNT(*) FROM {t} {whereSql}";

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        return await conn.ExecuteScalarAsync<int>(sqlText, args);
    }

    public async Task<LogItem?> GetAsync(Guid id)
    {
        var t = TableName();
        var sqlText = $"SELECT * FROM {t} WHERE cluster_id = @ClusterId AND id = @Id";

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        var row = await conn.QueryFirstOrDefaultAsync<LogPersistenceRecord>(sqlText,
            new { ClusterId = ClusterConnConfig.ClusterId, Id = id });
        return row == null ? null : ToLogItem(row);
    }

    public async Task<int> DeleteByTimestampAsync(DateTime timestampTo, int limit)
    {
        if (limit <= 0) throw new ArgumentException("Limit must be > 0", nameof(limit));

        var t = TableName();
        var selectSql = new StringBuilder($"SELECT id FROM {t}\n");
        selectSql.Append("WHERE cluster_id = @ClusterId AND timestamp_utc <= @TimestampTo\n");
        selectSql.Append("ORDER BY timestamp_utc ASC, id ASC\n");
        selectSql.Append(sql.OffsetQueryFor(limit, 0));

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var ids = (await conn.QueryAsync<Guid>(selectSql.ToString(), new
            {
                ClusterId = ClusterConnConfig.ClusterId,
                TimestampTo = DateTime.SpecifyKind(timestampTo, DateTimeKind.Utc)
            }, tx)).ToList();

            if (ids.Count == 0)
            {
                tx.Commit();
                return 0;
            }

            var deleteSql = $"DELETE FROM {t} WHERE cluster_id = @ClusterId AND {sql.InClauseFor("id", "@Ids")}";
            await conn.ExecuteAsync(deleteSql, new { ClusterId = ClusterConnConfig.ClusterId, Ids = ids }, tx);

            tx.Commit();
            return ids.Count;
        }
        catch
        {
            tx.SafeRollback();
            throw;
        }
    }

    private (StringBuilder sb, DynamicParameters dynParams) BuildBulkInsertSql(string t, IList<LogItem> items, int offset, int count)
    {
        const string cols = "cluster_id, id, level, message, category, reference_id, timestamp_utc, host, source_member, source_file, source_line";
        var sb = new StringBuilder($"INSERT INTO {t} ({cols}) VALUES \n");
        var dynParams = new DynamicParameters();

        for (int i = 0; i < count; i++)
        {
            var item = items[offset + i];
            sb.Append($"(@ClusterId_{i}, @Id_{i}, @Level_{i}, @Message_{i}, @Category_{i}, @ReferenceId_{i}, @TimestampUtc_{i}, @Host_{i}, @SourceMember_{i}, @SourceFile_{i}, @SourceLine_{i})");
            if (i < count - 1) sb.Append(",\n");

            dynParams.Add($"ClusterId_{i}", ClusterConnConfig.ClusterId);
            dynParams.Add($"Id_{i}", item.Id);
            dynParams.Add($"Level_{i}", (int)item.Level);
            dynParams.Add($"Message_{i}", item.Message);
            dynParams.Add($"Category_{i}", item.Category?.ToString());
            dynParams.Add($"ReferenceId_{i}", item.ReferenceId);
            dynParams.Add($"TimestampUtc_{i}", DateTime.SpecifyKind(item.TimestampUtc, DateTimeKind.Utc));
            dynParams.Add($"Host_{i}", item.Host);
            dynParams.Add($"SourceMember_{i}", item.SourceMember);
            dynParams.Add($"SourceFile_{i}", item.SourceFile);
            dynParams.Add($"SourceLine_{i}", item.SourceLine);
        }

        return (sb, dynParams);
    }

    private (string whereSql, Dictionary<string, object?> args) BuildWhere(LogItemQueryCriteria criteria)
    {
        var where = new List<string> { "cluster_id = @ClusterId" };
        var args = new Dictionary<string, object?> { ["ClusterId"] = ClusterConnConfig.ClusterId };

        if (criteria.Level.HasValue)
        {
            where.Add("level = @Level");
            args["Level"] = (int)criteria.Level.Value;
        }

        if (criteria.Category.HasValue)
        {
            where.Add("category = @Category");
            args["Category"] = criteria.Category.Value.ToString();
        }

        if (!string.IsNullOrEmpty(criteria.ReferenceId))
        {
            where.Add("reference_id = @ReferenceId");
            args["ReferenceId"] = criteria.ReferenceId;
        }

        if (criteria.FromTimestamp.HasValue)
        {
            where.Add("timestamp_utc >= @FromTimestamp");
            args["FromTimestamp"] = DateTime.SpecifyKind(criteria.FromTimestamp.Value, DateTimeKind.Utc);
        }

        if (criteria.ToTimestamp.HasValue)
        {
            where.Add("timestamp_utc <= @ToTimestamp");
            args["ToTimestamp"] = DateTime.SpecifyKind(criteria.ToTimestamp.Value, DateTimeKind.Utc);
        }

        if (!string.IsNullOrEmpty(criteria.Keyword))
        {
            where.Add(BuildKeywordFilter());
            args["Keyword"] = $"%{criteria.Keyword}%";
        }

        return ($"WHERE {string.Join(" AND ", where)}", args);
    }

    private static string BuildKeywordFilter() => "message LIKE @Keyword";

    private static LogItem ToLogItem(LogPersistenceRecord r)
    {
        JobMasterLogCategory? category = null;
        if (!string.IsNullOrEmpty(r.Category) &&
            Enum.TryParse<JobMasterLogCategory>(r.Category, ignoreCase: true, out var parsed))
        {
            category = parsed;
        }

        return new LogItem
        {
            ClusterId = r.ClusterId,
            Id = r.Id,
            Level = (JobMasterLogLevel)r.Level,
            Message = r.Message,
            Category = category,
            ReferenceId = r.ReferenceId,
            TimestampUtc = r.TimestampUtc,
            Host = r.Host,
            SourceMember = r.SourceMember,
            SourceFile = r.SourceFile,
            SourceLine = r.SourceLine
        };
    }

    private sealed class LogPersistenceRecord
    {
        public string ClusterId { get; set; } = string.Empty;
        public Guid Id { get; set; }
        public int Level { get; set; }
        public string Message { get; set; } = string.Empty;
        public string? Category { get; set; }
        public string? ReferenceId { get; set; }
        public DateTime TimestampUtc { get; set; }
        public string? Host { get; set; }
        public string? SourceMember { get; set; }
        public string? SourceFile { get; set; }
        public int? SourceLine { get; set; }
    }
}
