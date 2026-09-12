using Npgsql;

namespace JobMaster.Benchmarks.Common.Containers;

/// <summary>Creates the per-worker databases (master + each dedicated agent connection) before the
/// JobMaster host containers start -- Postgres requires the database to already exist, unlike
/// JobMaster's own schema-per-database auto-migration which happens inside the app on startup.
/// Mirrors <c>Tests/JobMaster.ScenarioTests/Runner/PostgresDatabaseProvisioner.cs</c>.</summary>
public static class PostgresDatabaseProvisioner
{
    private const string DuplicateDatabaseSqlState = "42P04";

    public static async Task CreateDatabasesIfNotExistsAsync(string adminConnectionString, IEnumerable<string> databaseNames, CancellationToken ct = default)
    {
        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync(ct);
        foreach (var databaseName in databaseNames)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE \"{databaseName}\"";

            try
            {
                await command.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException ex) when (ex.SqlState == DuplicateDatabaseSqlState)
            {
                // Already provisioned by an earlier run reusing the same containers.
            }
        }
    }
}
