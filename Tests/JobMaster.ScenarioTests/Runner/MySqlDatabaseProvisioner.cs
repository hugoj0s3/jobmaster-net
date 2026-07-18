using MySqlConnector;

namespace JobMaster.ScenarioTests.Runner;

internal static class MySqlDatabaseProvisioner
{
    public static async Task CreateDatabasesIfNotExistsAsync(string adminConnectionString, IEnumerable<string> databaseNames, CancellationToken ct = default)
    {
        await using var connection = new MySqlConnection(adminConnectionString);
        await connection.OpenAsync(ct);
        foreach (var databaseName in databaseNames)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE IF NOT EXISTS `{databaseName}`;";
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
