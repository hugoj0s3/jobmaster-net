using Microsoft.Data.SqlClient;

namespace JobMaster.ScenarioTests.Runner;

internal static class SqlServerDatabaseProvisioner
{
    public static readonly IReadOnlyCollection<string> DatabaseNames = new List<string>()
    {
        "SqlServerStandalone",
        "SqlServerDistCluster",
        "SqlServerAgent1",
        "SqlServerAgent2",
        "SqlServerAgent3",
    }.AsReadOnly();

    public static async Task CreateDatabasesIfNotExistsAsync(string adminConnectionString, CancellationToken ct = default)
    {
        await using var connection = new SqlConnection(adminConnectionString);
        await connection.OpenAsync(ct);
        foreach (var databaseName in DatabaseNames)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = @"
IF DB_ID(@dbName) IS NULL
BEGIN
    DECLARE @sql nvarchar(max) = N'CREATE DATABASE ' + QUOTENAME(@dbName) + N';';
    EXEC sp_executesql @sql;
END";
            command.Parameters.AddWithValue("@dbName", databaseName);
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
