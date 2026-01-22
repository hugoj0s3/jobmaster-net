using JobMaster.SqlBase.Scripts;

namespace JobMaster.SqlServer;

public class SqlServerSqlGenerator : SqlGenerator
{
    private const int MaxVarBinaryLength = 1024;

    protected override int MaxIdentifierLength => 128;

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
            var nullableSuffix2 = nullable ? string.Empty : " NOT NULL";
            if (isMaxLength)
            {
                return $"varbinary(max){nullableSuffix2}";
            }

            var requestedLength = length.GetValueOrDefault(MaxVarBinaryLength);
            var safeLength = Math.Min(requestedLength, MaxVarBinaryLength);
            return $"varbinary({safeLength}){nullableSuffix2}";
        }

        if (type != typeof(string))
        {
            return base.ColumnTypeFor(type, length, isMaxLength, nullable, precision, scale);
        }

        var nullableSuffix = nullable ? string.Empty : " NOT NULL";

        if (isMaxLength)
        {
            return $"nvarchar(max){nullableSuffix}";
        }

        return $"nvarchar({length}){nullableSuffix}";
    }

    public override string OffsetQueryFor(int limit, int offset = 0)
    {
        return $" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY ";
    }

    public override string TableExistsSql(string tablePrefix, string table)
    {
        // Note: tablePrefix is part of the table name in our system.
        var fullTableName = $"{tablePrefix}{table}";
        return $"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{fullTableName}'";
    }

    public override string InClauseFor(string columnName, string parameterName)
    {
        // Dapper expands list parameters for SQL Server in the form: "... WHERE col IN @Ids"
        return $"{columnName} IN {parameterName}";
    }

    public override string IdentityColumn()
    {
        return "IDENTITY(1,1)";
    }

    public override string GenerateVersionSql()
    {
        return "CAST(NEWID() AS NVARCHAR(36))";
    }

    public override string CreateIndex(string tableName, string indexName, params (string ColumnName, bool IsMaxLength, int? Length)[] columns)
    {
        indexName = NormalizeAndCapIdentifier(indexName, MaxIdentifierLength);
        var colsBuilder = new List<string>();
        var commentsSkipped = new List<string>();
        
        foreach (var (colName, IsMaxLength, length) in columns)
        {
            if (IsMaxLength)
            {
                commentsSkipped.Add($"Column {colName} is nvarchar(max)");
                continue; 
            }
            
            if (length.HasValue && length.Value > 450)
            {
                commentsSkipped.Add($"Column {colName} is too long");
                continue;
            }

            colsBuilder.Add(colName);
        }
        var comments = string.Join("\n", commentsSkipped);
        if (colsBuilder.Count > 0)
        {
            var cols = string.Join(", ", colsBuilder);
            
            return $@"
/*
{comments}
*/
CREATE INDEX {indexName} ON {tableName} ({cols});
";
        }
        
        return $"/*{comments}*/";
    }

    public override string RepositoryTypeId => SqlServerRepositoryConstants.RepositoryTypeId;

    protected override IDictionary<Type, string> TypeToSqlTypeMap => new Dictionary<Type, string>
    {
        { typeof(string), "nvarchar" },
        { typeof(int), "int" },
        { typeof(long), "bigint" },
        { typeof(DateTime), "datetime2(7)" },
        { typeof(bool), "bit" },
        { typeof(decimal), "decimal" },
        { typeof(double), "float" },
        { typeof(float), "real" },
        { typeof(Guid), "uniqueidentifier" },
        { typeof(byte[]), "varbinary" }
    };
}
