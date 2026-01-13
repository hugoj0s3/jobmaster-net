using JobMaster.Sql.Scripts;

namespace JobMaster.MySql;

public class MySqlSqlGenerator : SqlGenerator
{
    private const int MaxVarcharLength = 512;
    private const int MaxVarBinaryLength = 1024;

    protected override int MaxIdentifierLength => 64;

    public override string ColumnTypeFor(
        Type type,
        int? length = null,
        bool isMaxLength = false,
        bool nullable = true,
        int? precision = null,
        int? scale = null)
    {
        if (type == typeof(byte[]))
        {
            // MySQL cannot index BLOB without a prefix length. Some of our schema scripts index
            // value_binary, so keep it bounded.
            var nullableSuffix2 = nullable ? string.Empty : " NOT NULL";
            var requestedLength = length.GetValueOrDefault(MaxVarBinaryLength);
            var safeLength = Math.Min(requestedLength, MaxVarBinaryLength);
            return $"varbinary({safeLength}){nullableSuffix2}";
        }

        if (type != typeof(string))
            return base.ColumnTypeFor(type, length, isMaxLength, nullable, precision, scale);

        var nullableSuffix = nullable ? string.Empty : " NOT NULL";

        if (isMaxLength)
        {
            return $"longtext{nullableSuffix}";
        }

        if (length.HasValue && length.Value > 0)
        {
            return $"varchar({length.Value}){nullableSuffix}";
        }

        return $"longtext{nullableSuffix}";
    }

    public override string OffsetQueryFor(int limit, int offset = 0)
    {
        return $" LIMIT {limit} OFFSET {offset} ";
    }

    public override string TableExistsSql(string tablePrefix, string table)
    {
        var fullTableName = $"{tablePrefix}{table}";
        return $"SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND lower(table_name) = lower('{fullTableName}') LIMIT 1;";
    }

    public override string InClauseFor(string columnName, string parameterName)
    {
        // Dapper expands list parameters for MySQL in the form: "... WHERE col IN @Ids"
        return $"{columnName} IN {parameterName}";
    }

    public override string IdentityColumn()
    {
        return "AUTO_INCREMENT";
    }
    
    public override string GenerateVersionSql()
    {
        return "UUID()";
    }

    public override string RepositoryTypeId => MySqlRepositoryConstants.RepositoryTypeId;

    protected override IDictionary<Type, string> TypeToSqlTypeMap => new Dictionary<Type, string>
    {
        { typeof(string), "varchar" },
        { typeof(int), "int" },
        { typeof(long), "bigint" },
        { typeof(DateTime), "datetime(6)" },
        { typeof(bool), "tinyint" },
        { typeof(decimal), "decimal" },
        { typeof(double), "double" },
        { typeof(float), "float" },
        { typeof(Guid), "char(36)" },
        { typeof(byte[]), "varbinary" }
    };
    
    public override string CreateIndex(string tableName, string indexName, params (string ColumnName, bool IsMaxLength, int? Length)[] columns)
    {
        indexName = NormalizeAndCapIdentifier(indexName, MaxIdentifierLength);
        var colsBuilder = new List<string>();

        foreach (var (colName, isLargeText, length) in columns)
        {
            if (isLargeText || length > MaxVarcharLength)
            {
                // O Hack do MySQL fica isolado aqui!
                colsBuilder.Add($"{colName}({MaxVarcharLength})");
            }
            else
            {
                colsBuilder.Add(colName);
            }
        }

        var cols = string.Join(", ", colsBuilder);
        return $"CREATE INDEX {indexName} ON {tableName} ({cols});";
    }
}
