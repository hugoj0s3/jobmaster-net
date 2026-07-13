using MySqlConnector;

namespace JobMaster.ScenarioTests.Runner;

internal static class MySqlDatabaseProvisioner
{
    public static readonly IReadOnlyCollection<string> DatabaseNames = new List<string>()
    {
        "MySqlStandalone",
        "MySqlDistCluster",
        "MySqlAgent1",
        "MySqlAgent2",
        "MySqlAgent3",
    }.AsReadOnly();

    public static async Task CreateDatabasesIfNotExistsAsync(string adminConnectionString, CancellationToken ct = default)
    {
        await using var connection = new MySqlConnection(adminConnectionString);
        await connection.OpenAsync(ct);
        foreach (var databaseName in DatabaseNames)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE IF NOT EXISTS `{databaseName}`;";
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
