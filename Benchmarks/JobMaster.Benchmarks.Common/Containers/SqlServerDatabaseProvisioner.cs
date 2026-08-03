using Microsoft.Data.SqlClient;

namespace JobMaster.Benchmarks.Common.Containers;

/// <summary>Creates databases before host containers start -- SQL Server requires the database to
/// already exist, same rationale as <see cref="PostgresDatabaseProvisioner"/>.</summary>
public static class SqlServerDatabaseProvisioner
{
    private const int DatabaseAlreadyExistsErrorNumber = 1801;

    public static async Task CreateDatabasesIfNotExistsAsync(string adminConnectionString, IEnumerable<string> databaseNames, CancellationToken ct = default)
    {
        await using var connection = new SqlConnection(adminConnectionString);
        await connection.OpenAsync(ct);
        foreach (var databaseName in databaseNames)
        {
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = $"CREATE DATABASE [{databaseName}]";

                try
                {
                    await command.ExecuteNonQueryAsync(ct);
                }
                catch (SqlException ex) when (ex.Number == DatabaseAlreadyExistsErrorNumber)
                {
                    // Already provisioned by an earlier run reusing the same containers.
                }
            }

            // READ_COMMITTED_SNAPSHOT makes readers use row versioning instead of shared locks under
            // the default READ COMMITTED isolation level -- confirmed via a real benchmark run that
            // multiple clustered app instances hammering the same tables (e.g. Quartz.NET's AdoJobStore
            // trigger-acquisition polling) produce real SQL Server deadlocks otherwise. Safe to run
            // unconditionally here (right after CREATE DATABASE, before anything else connects) since
            // ALTER DATABASE just needs no other active connections at that moment, which nothing yet
            // has. ALTER DATABASE can't run inside a multi-statement batch with other DDL, so it's its
            // own command against a database with no rows/schema yet to lock.
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = $"ALTER DATABASE [{databaseName}] SET READ_COMMITTED_SNAPSHOT ON";
                await command.ExecuteNonQueryAsync(ct);
            }
        }
    }
}
