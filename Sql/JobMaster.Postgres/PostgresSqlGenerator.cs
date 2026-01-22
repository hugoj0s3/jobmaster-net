using JobMaster.SqlBase.Scripts;

namespace JobMaster.Postgres;

internal class PostgresSqlGenerator : SqlGenerator
{
    protected override int MaxIdentifierLength => 63;

    public override string ColumnTypeFor(
        Type type,
        int? length = null,
        bool isMaxLength = false,
        bool nullable = true,
        int? precision = null,
        int? scale = null)
    {
        // Only customize string; defer to base for everything else
        if (type != typeof(string))
            return base.ColumnTypeFor(type, length, isMaxLength, nullable, precision, scale);

        var nullableSuffix = nullable ? string.Empty : " NOT NULL";

        // “max” => Postgres idiom is text
        if (isMaxLength)
            return $"text{nullableSuffix}";

        // If a valid length is provided, respect it; otherwise use text
        if (length.HasValue && length.Value > 0)
            return $"varchar({length.Value}){nullableSuffix}";

        return $"text{nullableSuffix}";
    }

    public override string OffsetQueryFor(int limit, int offset = 0)
    {
        return $" LIMIT {limit} OFFSET {offset} ";
    }

    public override string TableExistsSql(string tablePrefix, string table)
    {
        var fullTableName = $"{tablePrefix}{table}".ToLowerInvariant();
        return $"SELECT 1 FROM information_schema.tables WHERE lower(table_name) = '{fullTableName}' AND table_schema = current_schema() LIMIT 1;";
    }
    
    public override string InClauseFor(string columnName, string parameterName)
    {
        return $"{columnName} =ANY({parameterName})";
    }

    public override string IdentityColumn()
    {
        return "GENERATED ALWAYS AS IDENTITY";
    }
    
    public override string GenerateVersionSql()
    {
        return "gen_random_uuid()::text";
    }

    public override string RepositoryTypeId => PostgresRepositoryConstants.RepositoryTypeId;
    protected override IDictionary<Type, string> TypeToSqlTypeMap => new Dictionary<Type, string>
    {
        { typeof(string), "varchar" },
        { typeof(int), "integer" },
        { typeof(long), "bigint" },
        { typeof(DateTime), "timestamp without time zone" },
        { typeof(bool), "boolean" },
        { typeof(decimal), "numeric" },
        { typeof(double), "double precision" },
        { typeof(float), "real" },
        { typeof(Guid), "uuid" },
        { typeof(byte[]), "bytea" }
    };
}