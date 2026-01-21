using MySqlConnector;

namespace JobMaster.IntegrationTests.Utils;

public static class MySqlTestDbUtil
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

        await using var cnn = new MySqlConnection(connectionString);
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

        await using var cnn = new MySqlConnection(connectionString);
        await cnn.OpenAsync();

        await DropTableIfExistsAsync(cnn, $"{tablePrefix}message_dispatcher");
        await DropTableIfExistsAsync(cnn, $"{tablePrefix}bucket_dispatcher");
    }

    private static async Task DropTableIfExistsAsync(MySqlConnection cnn, string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            return;
        }

        var sql = $"DROP TABLE IF EXISTS {tableName};";
        await using var cmd = new MySqlCommand(sql, cnn);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task EnsureDatabaseExistsAsync(string connectionString)
    {
        var builder = new MySqlConnectionStringBuilder(connectionString);
        var databaseName = builder.Database;
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            return;
        }

        var adminBuilder = new MySqlConnectionStringBuilder(connectionString)
        {
            Database = string.Empty
        };

        await using var adminConn = new MySqlConnection(adminBuilder.ConnectionString);
        await adminConn.OpenAsync();

        var sql = $"CREATE DATABASE IF NOT EXISTS {EscapeIdentifier(databaseName)};";
        await using var cmd = new MySqlCommand(sql, adminConn);
        await cmd.ExecuteNonQueryAsync();
    }

    private static string EscapeIdentifier(string identifier)
    {
        return $"`{identifier.Replace("`", "``")}`";
    }
}
