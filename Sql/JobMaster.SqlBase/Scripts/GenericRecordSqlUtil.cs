using System.Data;
using System.Text;
using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Serialization;

namespace JobMaster.SqlBase.Scripts;

public class GenericRecordSqlUtil
{
    private readonly ISqlGenerator sql;
    private readonly JobMasterConfigDictionary additionalConnConfig;
    private readonly string clusterId;

    public GenericRecordSqlUtil(ISqlGenerator sql, JobMasterConfigDictionary additionalConnConfig, string clusterId)
    {
        this.sql = sql;
        this.additionalConnConfig = additionalConnConfig;
        this.clusterId = clusterId;
    }
    
    public (string Sql, object Args) BuildGetSql(string groupId, string entryId, bool includeExpired)
    {
        var baseSelectSql = BaseSelectSql();
        var uniqueId = GenericRecordEntry.UniqueId(clusterId, groupId, entryId);

        var sql = $@"
{baseSelectSql}
where {EntryTable()}.{Col(x => x.RecordUniqueId)} = @UniqueId
";
        if (!includeExpired)
        {
            sql += $"and ( {Col(x => x.ExpiresAt)} IS NULL or {Col(x => x.ExpiresAt)} > @NowUtc)";
        }
        
        return (sql, new {UniqueId = uniqueId, NowUtc = DateTime.UtcNow });
    }
    
    public string BaseSelectSql()
    {
        var t = EntryTable();
        var cRecordId    = EntryTable() + "." + Col(x => x.RecordUniqueId);
        var cClusterId   = Col(x => x.ClusterId);
        var cGroupId     = Col(x => x.GroupId);
        var cEntryId     = Col(x => x.EntryId);
        var cSubjectType = Col(x => x.SubjectType);
        var cSubjectId   = Col(x => x.SubjectId);
        var cCreatedAt   = Col(x => x.CreatedAt);
        var cExpiresAt   = Col(x => x.ExpiresAt);

        return $@"
SELECT {cRecordId},
       {cClusterId},
       {cGroupId},
       {cEntryId},
       {cSubjectType},
       {cSubjectId},
       {cCreatedAt},
       {cExpiresAt},
       {ColVal(x => x.KeyName)},
       {ColVal(x => x.ValueText)},
       {ColVal(x => x.ValueBinary)},
       {ColVal(x => x.ValueInt64)},
       {ColVal(x => x.ValueBool)},
       {ColVal(x => x.ValueDecimal)},
       {ColVal(x => x.ValueDateTime)},
       {ColVal(x => x.ValueGuid)}
FROM {t}
left join {EntryValueTable()} on {EntryValueTable()}.{ColVal(x => x.RecordUniqueId)} = {t}.{ColVal(x => x.RecordUniqueId)}";
    }
    
    public (string Sql, object Args) BuildQuerySql(string groupId, GenericRecordQueryCriteria criteria)
    {
        // This query pages *entries*, not value rows.
        // We first select an ordered/paged set of entries in a CTE (aliased as `base`),
        // then join values.
        var cRecordId    = $"e.{Col(x => x.RecordUniqueId)}";
        var cClusterId   = $"e.{Col(x => x.ClusterId)}";
        var cGroupId     = $"e.{Col(x => x.GroupId)}";
        var cEntryId     = $"e.{Col(x => x.EntryId)}";
        var cSubjectType = $"e.{Col(x => x.SubjectType)}";
        var cSubjectId   = $"e.{Col(x => x.SubjectId)}";
        var cCreatedAt   = $"e.{Col(x => x.CreatedAt)}";
        var cExpiresAt   = $"e.{Col(x => x.ExpiresAt)}";

        var where = new List<string>
        {
            $"{cClusterId} = @ClusterId",
            $"{cGroupId} = @GroupId"
        };

        if (!criteria.IncludeExpired)
            where.Add($"({cExpiresAt} IS NULL OR {cExpiresAt} > @NowUtc)");
        if (!string.IsNullOrEmpty(criteria.SubjectType))
            where.Add($"{cSubjectType} = @SubjectType");
        if (criteria.SubjectIds.Any())
            where.Add(this.sql.InClauseFor(cSubjectId, "@SubjectIds"));
        if (criteria.EntryIds.Any())
            where.Add(this.sql.InClauseFor(cEntryId, "@EntryIds"));
        if (criteria.CreatedAtFrom.HasValue)
            where.Add($"{cCreatedAt} >= @CreatedAtFrom");
        if (criteria.CreatedAtTo.HasValue)
            where.Add($"{cCreatedAt} <= @CreatedAtTo");
        if (criteria.ExpiresAtFrom.HasValue)
            where.Add($"{cExpiresAt} >= @ExpiresAtFrom");
        if (criteria.ExpiresAtTo.HasValue)
            where.Add($"{cExpiresAt} <= @ExpiresAtTo");
        
        var args = new Dictionary<string, object?>
        {
            { "ClusterId", clusterId },
            { "GroupId", groupId },
            { "SubjectIds", criteria.SubjectIds },
            { "EntryIds", criteria.EntryIds.ToArray() },
            { "CreatedAtFrom", criteria.CreatedAtFrom },
            { "CreatedAtTo", criteria.CreatedAtTo },
            { "ExpiresAtFrom", criteria.ExpiresAtFrom },
            { "ExpiresAtTo", criteria.ExpiresAtTo },
            { "SubjectType", criteria.SubjectType },
            { "NowUtc", DateTime.UtcNow }
        };

        var exists = BuildWhereClause(criteria.Filters, "e", "v2", args);
        if (!string.IsNullOrEmpty(exists)) 
            where.Add(exists);

        var orderBy = criteria.OrderBy switch
        {
            GenericRecordQueryOrderByTypeId.CreatedAtAsc  => $"{cCreatedAt} ASC, {cRecordId} ASC",
            GenericRecordQueryOrderByTypeId.CreatedAtDesc => $"{cCreatedAt} DESC, {cRecordId} DESC",
            GenericRecordQueryOrderByTypeId.ExpiresAtAsc  => $"{cExpiresAt} ASC, {cRecordId} ASC",
            GenericRecordQueryOrderByTypeId.ExpiresAtDesc => $"{cExpiresAt} DESC, {cRecordId} DESC",
            _ => $"{cCreatedAt} DESC, {cRecordId} DESC"
        };

        var baseOrderBy = criteria.OrderBy switch
        {
            GenericRecordQueryOrderByTypeId.CreatedAtAsc  => $"base.{Col(x => x.CreatedAt)} ASC, base.{Col(x => x.RecordUniqueId)} ASC",
            GenericRecordQueryOrderByTypeId.CreatedAtDesc => $"base.{Col(x => x.CreatedAt)} DESC, base.{Col(x => x.RecordUniqueId)} DESC",
            GenericRecordQueryOrderByTypeId.ExpiresAtAsc  => $"base.{Col(x => x.ExpiresAt)} ASC, base.{Col(x => x.RecordUniqueId)} ASC",
            GenericRecordQueryOrderByTypeId.ExpiresAtDesc => $"base.{Col(x => x.ExpiresAt)} DESC, base.{Col(x => x.RecordUniqueId)} DESC",
            _ => $"base.{Col(x => x.CreatedAt)} DESC, base.{Col(x => x.RecordUniqueId)} DESC"
        };

        var t = EntryTable();
        var vt = EntryValueTable();

        var baseSelect = $@"
WITH base AS (
    SELECT
       {Col(x => x.RecordUniqueId)},
       {Col(x => x.ClusterId)},
       {Col(x => x.GroupId)},
       {Col(x => x.EntryId)},
       {Col(x => x.SubjectType)},
       {Col(x => x.SubjectId)},
       {Col(x => x.CreatedAt)},
       {Col(x => x.ExpiresAt)}
    FROM {t} e
    WHERE {string.Join(" AND ", where)}
";

        // SQL Server does not allow ORDER BY inside a CTE unless it is paired with TOP/OFFSET.
        // We only need ordering inside the CTE when we page the base entry set.
        if (criteria.Limit.HasValue)
        {
            baseSelect += $"\nORDER BY {orderBy}";
            baseSelect += "\n" + sql.OffsetQueryFor(criteria.Limit.Value, criteria.Offset ?? 0);
        }

        baseSelect += $@"
)
SELECT base.{Col(x => x.RecordUniqueId)},
       base.{Col(x => x.ClusterId)},
       base.{Col(x => x.GroupId)},
       base.{Col(x => x.EntryId)},
       base.{Col(x => x.SubjectType)},
       base.{Col(x => x.SubjectId)},
       base.{Col(x => x.CreatedAt)},
       base.{Col(x => x.ExpiresAt)},
       v.{ColVal(x => x.KeyName)},
       v.{ColVal(x => x.ValueText)},
       v.{ColVal(x => x.ValueBinary)},
       v.{ColVal(x => x.ValueInt64)},
       v.{ColVal(x => x.ValueBool)},
       v.{ColVal(x => x.ValueDecimal)},
       v.{ColVal(x => x.ValueDateTime)},
       v.{ColVal(x => x.ValueGuid)}
FROM base
LEFT JOIN {vt} v ON v.{ColVal(x => x.RecordUniqueId)} = base.{Col(x => x.RecordUniqueId)}
ORDER BY {baseOrderBy}
";

        return (baseSelect, args);
    }
    
    public IList<(string, IDictionary<string, object?>)> BuildFilterArgs(IList<GenericRecordValueFilter> filters, string alias)
    {
        var result = new List<(string, IDictionary<string, object?>)>();
        for (int i = 0; i < filters.Count; i++)
        {
            var (clause, arg) = ToSqlFilter(filters[i], i, alias);
            result.Add((clause, arg));
        }
        
        return result;
    }
    
    public (string, IDictionary<string, object?>) ToSqlFilter(GenericRecordValueFilter filter, int index, string alias)
    {
        if (filter.Value == null && filter.Values?.Any() != true)
        {
            return (string.Empty, new Dictionary<string, object?>());
        }

        object? sampleValue = filter.Value;
        if (sampleValue == null && filter.Values?.Any() == true)
        {
            sampleValue = filter.Values.FirstOrDefault(v => v != null);
        }
        if (sampleValue == null)
        {
            return (string.Empty, new Dictionary<string, object?>());
        }
        
        var cKeyName = ColVal(x => x.KeyName);
        var keyNameFilter = $"{alias}.{cKeyName} = @KeyName{index}";
        var fieldRelated = sampleValue switch
        {
            string s => $"{ColVal(x => x.ValueText)}",
            char s => $"{ColVal(x => x.ValueText)}",
            int i => $"{ColVal(x => x.ValueInt64)}",
            long l => $"{ColVal(x => x.ValueInt64)}",
            double d => $"{ColVal(x => x.ValueDecimal)}",
            decimal dec => $"{ColVal(x => x.ValueDecimal)}",
            bool b => $"{ColVal(x => x.ValueBool)}",
            DateTime dt => $"{ColVal(x => x.ValueDateTime)}",
            Guid g => $"{ColVal(x => x.ValueGuid)}",
            byte[] bin => $"{ColVal(x => x.ValueBinary)}",
            _ => $"{ColVal(x => x.ValueText)}"
        };
        
        fieldRelated = $"{alias}.{fieldRelated}";
        
        var initialClause = $"( {keyNameFilter} and {fieldRelated} is not null";
        var finalClause = string.Empty;
        if (filter.Operation == GenericFilterOperation.In)
        {
            finalClause = $" {initialClause} and {this.sql.InClauseFor(fieldRelated, $"@Values{index}")} )";
        } 
        else if (filter.Operation == GenericFilterOperation.Eq) 
        {
             finalClause = ($" {initialClause} and {fieldRelated} = @Value{index} )");
        }
        else if (filter.Operation == GenericFilterOperation.Neq)
        {
            finalClause = $" {initialClause} and {fieldRelated} != @Value{index} )";
        }
        else if (filter.Operation == GenericFilterOperation.Contains && filter.Value is string)
        {
            finalClause = $" {initialClause} and {fieldRelated} like concat('%', @Value{index}, '%') )";
        }
        else if (filter.Operation == GenericFilterOperation.EndsWith && filter.Value is string)
        {
            finalClause = $" {initialClause} and {fieldRelated} like concat('%', @Value{index}) )";
        }
        else if (filter.Operation == GenericFilterOperation.StartsWith && filter.Value is string) 
        {
            finalClause = $" {initialClause} and {fieldRelated} like concat(@Value{index}, '%') )";
        }
        else if (filter.Operation == GenericFilterOperation.Lt && filter.Value is not string)
        {
            finalClause = $" {initialClause} and {fieldRelated} < @Value{index} )";
        }
        else if (filter.Operation == GenericFilterOperation.Lte && filter.Value is not string)
        {
            finalClause = $" {initialClause} and {fieldRelated} <= @Value{index} )";
        }
        else if (filter.Operation == GenericFilterOperation.Gt && filter.Value is not string)
        {
            finalClause = $" {initialClause} and {fieldRelated} > @Value{index} )";
        }
        else if (filter.Operation == GenericFilterOperation.Gte && filter.Value is not string)
        {
            finalClause = $" {initialClause} and {fieldRelated} >= @Value{index} )";
        }
        else
        {
            finalClause = string.Empty;
        }

        if (string.IsNullOrEmpty(finalClause))
        {
            return (string.Empty, new Dictionary<string, object?>());
        }

        return (finalClause, new Dictionary<string, object?>()
        {
            { $"Value{index}", filter.Value ?? sampleValue },
            { $"Values{index}", CoerceValuesArray(sampleValue, filter.Values) },
            { $"KeyName{index}", filter.Key },
        });
    }

    private static object? CoerceValuesArray(object sampleValue, IReadOnlyList<object?>? values)
    {
        if (values is null)
        {
            return null;
        }

        // For IN/ANY we need strongly typed arrays for some providers (notably Npgsql).
        // We infer the type from the first non-null value (sampleValue).
        switch (sampleValue)
        {
            case string:
            case char:
                return values.Where(v => v != null).Select(v => v!.ToString()!).ToArray();
            case int:
            case long:
                return values.Where(v => v != null).Select(v => Convert.ToInt64(v)).ToArray();
            case double:
            case decimal:
            case float:
                return values.Where(v => v != null).Select(v => Convert.ToDecimal(v)).ToArray();
            case bool:
                return values.Where(v => v != null).Select(v => Convert.ToBoolean(v)).ToArray();
            case DateTime:
                return values.Where(v => v != null).Select(v => (DateTime)Convert.ChangeType(v, typeof(DateTime))!).ToArray();
            case Guid:
                return values.Where(v => v != null).Select(v => v is Guid g ? g : Guid.Parse(v!.ToString()!)).ToArray();
            case byte[]:
                return values.Where(v => v != null).Select(v => v as byte[] ?? Convert.FromBase64String(v!.ToString()!)).ToArray();
            default:
                // Fall back to the original values. This is best-effort.
                return values.ToArray();
        }
    }

    // Convenience: build EXISTS clause string for a list of value filters.
    // Also merges the generated parameter values into the provided args dictionary.
    public string BuildWhereClause(IList<GenericRecordValueFilter>? filters, string entryTableAlias, string entryValueTableAlias, IDictionary<string, object?> args)
    {
        if (filters is null || filters.Count == 0) return string.Empty;

        if (string.IsNullOrEmpty(entryTableAlias))
        {
            throw new ArgumentException("entryTableAlias cannot be null or empty");
        }

        if (string.IsNullOrEmpty(entryValueTableAlias))
        {
            throw new ArgumentException("entryValueTableAlias cannot be null or empty");
        }

        var filterArgs = BuildFilterArgs(filters, entryValueTableAlias);
        foreach (var kv in filterArgs.SelectMany(x => x.Item2))
        {
            // Allow later args to overwrite earlier ones if same key shows up
            args[kv.Key] = kv.Value;
        }
        var clauses = filterArgs.Select(x => x.Item1).Where(s => !string.IsNullOrWhiteSpace(s)).ToList();
        if (clauses.Count == 0) return string.Empty;

        var exists = $" exists (  " +
                     $"           select 1 " +
                     $"             from {EntryValueTable()} as {entryValueTableAlias} " +
                     $"           where {entryTableAlias}.{Col(x => x.RecordUniqueId)} = {entryValueTableAlias}.{ColVal(x => x.RecordUniqueId)}" +
                     $"           and {string.Join(" AND ", clauses)}" +
                     $"   ) ";

        return exists;
    }
    
    public string EntryTable() => sql.TableNameFor<GenericRecordEntry>(additionalConnConfig);

    public string EntryValueTable()
    {
        var tablePrefix = sql.GetTablePrefix(additionalConnConfig);
        var prefix = string.IsNullOrEmpty(tablePrefix) ? string.Empty : tablePrefix;
        return $"{prefix}generic_record_entry_value";
    }

    public string Col(System.Linq.Expressions.Expression<Func<GenericRecordEntry, object?>> prop) => sql.ColumnNameFor(prop);

    public string ColVal(System.Linq.Expressions.Expression<Func<SqlGenericRecordEntryValue, object?>> prop) => sql.ColumnNameFor(prop);
    public string ColSqlEntry(System.Linq.Expressions.Expression<Func<SqlGenericRecordEntry, object?>> prop) => sql.ColumnNameFor(prop);

    public (string Sql, IDictionary<string, object?> Args) BuildUpdateEntrySql(SqlGenericRecordEntry entry)
    {
        var t = EntryTable();
        var cRecordId    = EntryTable() + "." + Col(x => x.RecordUniqueId);
        var cSubjectType = Col(x => x.SubjectType);
        var cSubjectId   = Col(x => x.SubjectId);
        var cExpiresAt   = Col(x => x.ExpiresAt);
        // Note: not updating EntryId/ClusterId/GroupId/CreatedAt

        var args = new Dictionary<string, object?>
        {
            {"RecordUniqueId", entry.RecordUniqueId},
            {"SubjectType", entry.SubjectType},
            {"SubjectId", entry.SubjectId},
            {"ExpiresAt", entry.ExpiresAt}
        };

        var sb = new StringBuilder($@"UPDATE {t}
SET {cSubjectType} = @SubjectType,
    {cSubjectId} = @SubjectId,
    {cExpiresAt} = @ExpiresAt
WHERE {cRecordId} = @RecordUniqueId;");

        return (sb.ToString(), args);
    }

    public (string Sql, IDictionary<string, object?> Args) BuildInsertEntrySql(SqlGenericRecordEntry entry)
    {
        var t = EntryTable();
        // Include EntryIdGuid column; ensure your DDL has been updated accordingly
        var cols = $@"
{Col(x => x.RecordUniqueId)},
{Col(x => x.ClusterId)},
{Col(x => x.GroupId)},
{Col(x => x.EntryId)},
{ColSqlEntry(x => x.EntryIdGuid)},
{Col(x => x.SubjectType)},
{Col(x => x.SubjectId)},
{Col(x => x.CreatedAt)},
{Col(x => x.ExpiresAt)}";

        var args = new Dictionary<string, object?>
        {
            {"RecordUniqueId", entry.RecordUniqueId},
            {"ClusterId", entry.ClusterId},
            {"GroupId", entry.GroupId},
            {"EntryId", entry.EntryId},
            {"EntryIdGuid", entry.EntryIdGuid},
            {"SubjectType", entry.SubjectType},
            {"SubjectId", entry.SubjectId},
            {"CreatedAt", entry.CreatedAt},
            {"ExpiresAt", entry.ExpiresAt}
        };

        var sb = new StringBuilder($"INSERT INTO {t} ({cols}) ");
        sb.AppendLine("VALUES (@RecordUniqueId, @ClusterId, @GroupId, @EntryId, @EntryIdGuid, @SubjectType, @SubjectId, @CreatedAt, @ExpiresAt);");

        return (sb.ToString(), args);
    }

    private void InsertEntryValues(IDbConnection conn, IDbTransaction tx, SqlGenericRecordEntry entry)
    {
        if (entry.Values.Count == 0) return;

        var vt = EntryValueTable();
        var insertSql = $@"INSERT INTO {vt} (
{ColVal(x => x.RecordUniqueId)},
{ColVal(x => x.KeyName)},
{ColVal(x => x.ValueText)},
{ColVal(x => x.ValueBinary)},
{ColVal(x => x.ValueInt64)},
{ColVal(x => x.ValueBool)},
{ColVal(x => x.ValueDecimal)},
{ColVal(x => x.ValueDateTime)},
{ColVal(x => x.ValueGuid)})
VALUES (@RecordUniqueId, @KeyName, @ValueText, @ValueBinary, @ValueInt64, @ValueBoolean, @ValueDecimal, @ValueDateTime, @ValueGuid);";

        var rows = entry.Values.Select(v => new
        {
            RecordUniqueId = entry.RecordUniqueId,
            KeyName = v.KeyName,
            ValueText = v.ValueText,
            ValueBinary = v.ValueBinary,
            ValueInt64 = v.ValueInt64,
            ValueBoolean = v.ValueBool,
            ValueDecimal = v.ValueDecimal,
            ValueDateTime = v.ValueDateTime,
            ValueGuid = v.ValueGuid
        });

        conn.Execute(insertSql, rows, tx);
    }

    private async Task InsertEntryValuesAsync(IDbConnection conn, IDbTransaction tx, SqlGenericRecordEntry entry)
    {
        if (entry.Values.Count == 0) return;

        var vt = EntryValueTable();
        var insertSql = $@"INSERT INTO {vt} (
{ColVal(x => x.RecordUniqueId)},
{ColVal(x => x.KeyName)},
{ColVal(x => x.ValueText)},
{ColVal(x => x.ValueBinary)},
{ColVal(x => x.ValueInt64)},
{ColVal(x => x.ValueBool)},
{ColVal(x => x.ValueDecimal)},
{ColVal(x => x.ValueDateTime)},
{ColVal(x => x.ValueGuid)})
VALUES (@RecordUniqueId, @KeyName, @ValueText, @ValueBinary, @ValueInt64, @ValueBoolean, @ValueDecimal, @ValueDateTime, @ValueGuid);";

        var rows = entry.Values.Select(v => new
        {
            RecordUniqueId = entry.RecordUniqueId,
            KeyName = v.KeyName,
            ValueText = v.ValueText,
            ValueBinary = v.ValueBinary,
            ValueInt64 = v.ValueInt64,
            ValueBoolean = v.ValueBool,
            ValueDecimal = v.ValueDecimal,
            ValueDateTime = v.ValueDateTime,
            ValueGuid = v.ValueGuid
        });

        await conn.ExecuteAsync(insertSql, rows, tx);
    }
    
    

    public string BuildDeleteValuesSql(string idParamName = "@RecordUniqueId")
    {
        var vt = EntryValueTable();
        var cRecordId = Col(x => x.RecordUniqueId);
        return $"DELETE FROM {vt} WHERE {cRecordId} = {idParamName};";
    }

    public string BuildDeleteEntrySql(string idParamName = "@RecordUniqueId")
    {
        var t = EntryTable();
        var cRecordId = Col(x => x.RecordUniqueId);
        return $"DELETE FROM {t} WHERE {cRecordId} = {idParamName};";
    }
    
    public string BuildDeleteValuesMultipleSql(string idsParamName = "@RecordUniqueIds")
    {
        var vt = EntryValueTable();
        var cRecordId = Col(x => x.RecordUniqueId);
        var inClause = sql.InClauseFor(cRecordId, idsParamName);
        return $"DELETE FROM {vt} WHERE {inClause};";
    }

    public string BuildDeleteEntryMultipleSql(string idsParamName = "@RecordUniqueIds")
    {
        var t = EntryTable();
        var cRecordId = Col(x => x.RecordUniqueId);
        var inClause = sql.InClauseFor(cRecordId, idsParamName);
        return $"DELETE FROM {t} WHERE {inClause};";
    }
    
    public IList<GenericRecordEntry> LinearListToDomain(IEnumerable<SqlGenericRecordEntryLinearDto> result)
    {
        var dictionary = result.GroupBy(x => x.RecordUniqueId).ToDictionary(x => x.Key, x => x.ToList());
        var entries = MapLinearToSqlEntry(dictionary);
        return entries.Select(MapToEntry).ToList();
    }
    
    public IList<SqlGenericRecordEntry> MapLinearToSqlEntry(Dictionary<string, List<SqlGenericRecordEntryLinearDto>> linear)
    {
        var list = new List<SqlGenericRecordEntry>();
        foreach (var entry in linear)
        {
            var sqlEntry = new SqlGenericRecordEntry
            {
                RecordUniqueId = entry.Key,
                ClusterId = entry.Value[0].ClusterId,
                GroupId = entry.Value[0].GroupId,
                EntryId = entry.Value[0].EntryId,
                SubjectType = entry.Value[0].SubjectType,
                SubjectId = entry.Value[0].SubjectId,
                CreatedAt = entry.Value[0].CreatedAt,
                ExpiresAt = entry.Value[0].ExpiresAt
            };
            
            sqlEntry.Values = entry.Value.Select(x => new SqlGenericRecordEntryValue
            {
                RecordUniqueId = entry.Key,
                KeyName = x.KeyName,
                ValueText = x.ValueText,
                ValueBinary = x.ValueBinary,
                ValueInt64 = x.ValueInt64,
                ValueBool = x.ValueBool,
                ValueDecimal = x.ValueDecimal,
                ValueDateTime = x.ValueDateTime,
                ValueGuid = x.ValueGuid
            }).ToList();
            
            list.Add(sqlEntry);
        }
        return list;
    }
    
    public GenericRecordEntry MapToEntry(SqlGenericRecordEntry src)
    {
        var e = GenericRecordEntry.Create<object>( // payload reconstructed via Values later if needed
            src.ClusterId, src.GroupId, src.EntryId, (object)new { } // dummy to satisfy Create; values are separate
        );
        
        e.CreatedAt = src.CreatedAt;
        e.ExpiresAt = src.ExpiresAt;
        e.SubjectType = src.SubjectType;
        e.SubjectId = src.SubjectId;

        if (src.Values is { Count: > 0 })
        {
            var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var v in src.Values)
            {
                object? val =
                    v.ValueText ??
                    (object?)v.ValueBinary ??
                    v.ValueInt64 ??
                    v.ValueBool ??
                    (object?)v.ValueDecimal ??
                    (object?)v.ValueDateTime ??
                    v.ValueGuid;
                dict[v.KeyName] = val;
            }
            e.Values = dict;
        }

        return e;
    }
    
    public SqlGenericRecordEntry MapToSqlEntry(GenericRecordEntry src)
    {
        var entry = new SqlGenericRecordEntry
        {
            RecordUniqueId = src.RecordUniqueId,
            ClusterId = src.ClusterId,
            GroupId = src.GroupId,
            EntryId = src.EntryId,
            // Populate EntryIdGuid only when convertible (N format preferred)
            EntryIdGuid = Guid.TryParseExact(src.EntryId, "N", out var g) ? g : (Guid?)null,
            SubjectType = src.SubjectType,
            SubjectId = src.SubjectId,
            CreatedAt = src.CreatedAt,
            ExpiresAt = src.ExpiresAt
        };

        IList<SqlGenericRecordEntryValue> values = new List<SqlGenericRecordEntryValue>();
        foreach (var kv in src.Values)
        {
            var val = kv.Value;
            var row = new SqlGenericRecordEntryValue
            {
                RecordUniqueId = src.RecordUniqueId,
                KeyName = kv.Key
            };

            switch (val)
            {
                case null:
                    break;
                case string s:
                    row.ValueText = s; break;
                case char c:
                    row.ValueText = c.ToString(); break;
                case byte[] bin:
                    row.ValueBinary = bin; break;
                case bool b:
                    row.ValueBool = b; break;
                case sbyte or byte or short or ushort or int or uint or long or ulong:
                    row.ValueInt64 = Convert.ToInt64(val); break;
                case float or double or decimal:
                    row.ValueDecimal = Convert.ToDecimal(val); break;
                case DateTime dt:
                    row.ValueDateTime = DateTime.SpecifyKind(dt, DateTimeKind.Utc); break;
                case Guid gg:
                    row.ValueGuid = gg; break;
                default:
                    try
                    {
                        var json = InternalJobMasterSerializer.Serialize(val);
                        row.ValueText = json;
                    }
                    catch
                    {
                        // ignore JSON errors; leave row without a value
                    }
                    break;
            }

            values.Add(row);
        }

        entry.Values = values;
        return entry;
    }
    
    public (string, IList<object>) BuildInsertEntryValuesSql(SqlGenericRecordEntry entry)
    {
        var vt = EntryValueTable();
        var insertSql = $@"INSERT INTO {vt} (
{ColVal(x => x.RecordUniqueId)},
{ColVal(x => x.KeyName)},
{ColVal(x => x.ValueText)},
{ColVal(x => x.ValueBinary)},
{ColVal(x => x.ValueInt64)},
{ColVal(x => x.ValueBool)},
{ColVal(x => x.ValueDecimal)},
{ColVal(x => x.ValueDateTime)},
{ColVal(x => x.ValueGuid)})
VALUES (@RecordUniqueId, @KeyName, @ValueText, @ValueBinary, @ValueInt64, @ValueBoolean, @ValueDecimal, @ValueDateTime, @ValueGuid);";

        IList<object> result = new List<object>();
        foreach (var entryValue in entry.Values)
        {
            var row = new DynamicParameters();
            row.Add("RecordUniqueId", entryValue.RecordUniqueId);
            row.Add("KeyName", entryValue.KeyName);
            row.Add("ValueText", entryValue.ValueText);

            // Critical for SQL Server: ensure the parameter type is varbinary, even when null.
            row.Add("ValueBinary", entryValue.ValueBinary, dbType: DbType.Binary);

            row.Add("ValueInt64", entryValue.ValueInt64);
            row.Add("ValueBoolean", entryValue.ValueBool);
            row.Add("ValueDecimal", entryValue.ValueDecimal);
            row.Add("ValueDateTime", entryValue.ValueDateTime);
            row.Add("ValueGuid", entryValue.ValueGuid);
            result.Add(row);
        }

        return (insertSql, result);
    }
}
