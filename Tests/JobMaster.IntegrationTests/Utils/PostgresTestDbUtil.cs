using Microsoft.Extensions.Configuration;
using Npgsql;

namespace JobMaster.IntegrationTests.Utils;

public static class PostgresTestDbUtil
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

        await using var cnn = new NpgsqlConnection(connectionString);
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

        await using var cnn = new NpgsqlConnection(connectionString);
        await cnn.OpenAsync();

        await DropTableIfExistsAsync(cnn, $"{tablePrefix}message_dispatcher");
        await DropTableIfExistsAsync(cnn, $"{tablePrefix}bucket_dispatcher");
    }

    private static async Task DropTableIfExistsAsync(NpgsqlConnection cnn, string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            return;
        }

        var sql = $"DROP TABLE IF EXISTS {tableName} CASCADE;";
        await using var cmd = new NpgsqlCommand(sql, cnn);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task EnsureDatabaseExistsAsync(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var databaseName = builder.Database;
        if (string.IsNullOrWhiteSpace(databaseName) || databaseName.Equals("postgres", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var adminBuilder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Database = "postgres"
        };

        await using var adminConn = new NpgsqlConnection(adminBuilder.ConnectionString);
        await adminConn.OpenAsync();

        const string existsSql = "SELECT 1 FROM pg_database WHERE datname = @dbName;";
        await using (var existsCmd = new NpgsqlCommand(existsSql, adminConn))
        {
            existsCmd.Parameters.AddWithValue("dbName", databaseName);
            var exists = await existsCmd.ExecuteScalarAsync();
            if (exists is not null)
            {
                return;
            }
        }

        var createSql = $"CREATE DATABASE {EscapeIdentifier(databaseName)};";
        await using var createCmd = new NpgsqlCommand(createSql, adminConn);
        await createCmd.ExecuteNonQueryAsync();
    }

    private static string EscapeIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }
}
