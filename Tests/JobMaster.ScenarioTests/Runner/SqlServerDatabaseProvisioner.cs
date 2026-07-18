using Microsoft.Data.SqlClient;

namespace JobMaster.ScenarioTests.Runner;

internal static class SqlServerDatabaseProvisioner
{
    public static async Task CreateDatabasesIfNotExistsAsync(string adminConnectionString, IEnumerable<string> databaseNames, CancellationToken ct = default)
    {
        await using var connection = new SqlConnection(adminConnectionString);
        await connection.OpenAsync(ct);
        foreach (var databaseName in databaseNames)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = @"
IF DB_ID(@dbName) IS NULL
BEGIN
    DECLARE @sql nvarchar(max) = N'CREATE DATABASE ' + QUOTENAME(@dbName) + N';';
    EXEC sp_executesql @sql;

    -- JobMaster relies on non-blocking reads for counts/queries/heartbeats. RCSI gives READ
    -- COMMITTED readers a row-versioned snapshot instead of shared locks, so readers never block
    -- writers and vice versa (see JobMaster.SqlServer's README). Safe to enable unconditionally
    -- here since nothing has connected to the database yet.
    DECLARE @rcsiSql nvarchar(max) = N'ALTER DATABASE ' + QUOTENAME(@dbName) + N' SET READ_COMMITTED_SNAPSHOT ON;';
    EXEC sp_executesql @rcsiSql;
END";
            command.Parameters.AddWithValue("@dbName", databaseName);
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
