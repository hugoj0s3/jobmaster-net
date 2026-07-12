using Npgsql;

namespace JobMaster.ScenarioTests.Runner;

internal static class PostgresDatabaseProvisioner
{
    private const string DuplicateDatabaseSqlState = "42P04";

    public static readonly IReadOnlyCollection<string> DatabaseNames = new List<string>()
    {
        "PostgresStandalone",
    }.AsReadOnly();
    
    public static async Task CreateDatabasesIfNotExistsAsync(string adminConnectionString, CancellationToken ct = default)
    {
        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync(ct);
        foreach (var databaseName in DatabaseNames)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE \"{databaseName}\"";

            try
            {
                await command.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException ex) when (ex.SqlState == DuplicateDatabaseSqlState)
            {
                // Already provisioned by an earlier run of the same scenario within this test process.
            }
        }
    }
}
