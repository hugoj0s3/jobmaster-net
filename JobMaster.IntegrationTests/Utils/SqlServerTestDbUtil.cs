using Microsoft.Data.SqlClient;

namespace JobMaster.IntegrationTests.Utils;

public static class SqlServerTestDbUtil
{
    public static async Task DropJobMasterTablesAsync(
        string masterConnectionString,
        string masterTablePrefix,
        string agentConnectionString,
        string agentTablePrefix)
    {
        await DropMasterTablesAsync(masterConnectionString, masterTablePrefix);
        await DropAgentTablesAsync(agentConnectionString, agentTablePrefix);
    }

    public static async Task DropMasterTablesAsync(string connectionString, string tablePrefix)
    {
        await EnsureDatabaseExistsAsync(connectionString);

        await using var cnn = new SqlConnection(connectionString);
        await cnn.OpenAsync();

        await DropTableIfExistsAsync(cnn, $"{tablePrefix}generic_record_entry_value");
        await DropTableIfExistsAsync(cnn, $"{tablePrefix}generic_record_entry");
        await DropTableIfExistsAsync(cnn, $"{tablePrefix}distributed_lock");
        await DropTableIfExistsAsync(cnn, $"{tablePrefix}job");
        await DropTableIfExistsAsync(cnn, $"{tablePrefix}recurring_schedule");
    }

    public static async Task DropAgentTablesAsync(string connectionString, string tablePrefix)
    {
        await EnsureDatabaseExistsAsync(connectionString);

        await using var cnn = new SqlConnection(connectionString);
        await cnn.OpenAsync();

        await DropTableIfExistsAsync(cnn, $"{tablePrefix}message_dispatcher");
        await DropTableIfExistsAsync(cnn, $"{tablePrefix}bucket_dispatcher");
    }

    private static async Task DropTableIfExistsAsync(SqlConnection cnn, string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            return;
        }

        // Drop regardless of schema by looking up the table in sys.tables.
        // This avoids relying on the connection's default schema.
        var sql = @"
DECLARE @schema sysname;
SELECT TOP (1) @schema = s.name
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.name = @TableName;

IF @schema IS NOT NULL
BEGIN
    DECLARE @sql nvarchar(max) = N'DROP TABLE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@TableName) + N';';
    EXEC sp_executesql @sql;
END
";

        await using var cmd = new SqlCommand(sql, cnn);
        cmd.Parameters.AddWithValue("@TableName", tableName);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task EnsureDatabaseExistsAsync(string connectionString)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        var databaseName = builder.InitialCatalog;
        if (string.IsNullOrWhiteSpace(databaseName) || databaseName.Equals("master", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var masterBuilder = new SqlConnectionStringBuilder(builder.ConnectionString)
        {
            InitialCatalog = "master"
        };

        await using var masterConn = new SqlConnection(masterBuilder.ConnectionString);
        await masterConn.OpenAsync();

        var sql = @"
IF DB_ID(@dbName) IS NULL
BEGIN
    DECLARE @sql nvarchar(max) = N'CREATE DATABASE ' + QUOTENAME(@dbName) + N';';
    EXEC sp_executesql @sql;
END
";

        await using var cmd = new SqlCommand(sql, masterConn);
        cmd.Parameters.AddWithValue("@dbName", databaseName);
        await cmd.ExecuteNonQueryAsync();
    }

    private static string EscapeIdentifier(string identifier)
    {
        // Bracket escaping for SQL Server identifiers
        return $"[{identifier.Replace("]", "]]")}]";
    }
}
