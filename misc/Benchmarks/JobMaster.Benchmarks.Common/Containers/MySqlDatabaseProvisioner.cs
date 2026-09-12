using MySqlConnector;

namespace JobMaster.Benchmarks.Common.Containers;

/// <summary>Creates databases before host containers start -- MySQL requires the database to
/// already exist, same rationale as <see cref="PostgresDatabaseProvisioner"/>/
/// <see cref="SqlServerDatabaseProvisioner"/>. No RCSI-equivalent step needed here: InnoDB already
/// uses MVCC (undo-log row versioning) under its default REPEATABLE READ isolation, so readers don't
/// block writers the way SQL Server's default READ COMMITTED does without RCSI.</summary>
public static class MySqlDatabaseProvisioner
{
    public static async Task CreateDatabasesIfNotExistsAsync(string adminConnectionString, IEnumerable<string> databaseNames, CancellationToken ct = default)
    {
        await using var connection = new MySqlConnection(adminConnectionString);
        await connection.OpenAsync(ct);
        foreach (var databaseName in databaseNames)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE IF NOT EXISTS `{databaseName}`";
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
