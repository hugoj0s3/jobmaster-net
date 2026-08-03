using JobMaster.Benchmarks.Common.Containers;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;

namespace JobMaster.Benchmarks.QuartzRunner;

/// <summary>Creates Quartz.NET's `QRTZ_*` tables before any host container starts -- `AdoJobStore`
/// does not auto-create its schema (unlike JobMaster/Hangfire, which migrate themselves on startup).
/// Database creation itself is already handled generically by the per-engine
/// <c>*DatabaseProvisioner</c> classes; this only runs the vendored DDL script against it. No
/// existing analog in this repo -- Quartz is the first framework here that needs an
/// externally-applied schema.</summary>
public static class QuartzSchemaProvisioner
{
    public static async Task CreateSchemaAsync(DbEngine dbEngine, string adminConnectionString, CancellationToken ct = default)
    {
        var scriptFileName = dbEngine switch
        {
            DbEngine.Postgres => "tables_postgres.sql",
            DbEngine.MySql => "tables_mysql.sql",
            DbEngine.SqlServer => "tables_sqlServer.sql",
            _ => throw new ArgumentOutOfRangeException(nameof(dbEngine), dbEngine, null),
        };

        var scriptPath = Path.Combine(AppContext.BaseDirectory, "Sql", scriptFileName);
        var script = await File.ReadAllTextAsync(scriptPath, ct);

        switch (dbEngine)
        {
            case DbEngine.SqlServer:
                await RunSqlServerBatchesAsync(adminConnectionString, script, ct);
                break;
            case DbEngine.Postgres:
                await RunPostgresScriptAsync(adminConnectionString, script, ct);
                break;
            case DbEngine.MySql:
                await RunMySqlStatementsAsync(adminConnectionString, script, ct);
                break;
        }
    }

    private static async Task RunSqlServerBatchesAsync(string connectionString, string script, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);

        // "GO" is a SQLCMD/SSMS-only batch separator, not real T-SQL -- SqlCommand can't execute a
        // script containing it as a single ExecuteNonQuery call, so split and run each batch.
        var batches = script.Split(["\r\nGO\r\n", "\nGO\n", "\r\nGO", "\nGO"], StringSplitOptions.RemoveEmptyEntries);
        foreach (var batch in batches)
        {
            var trimmed = batch.Trim();
            if (trimmed.Length == 0) continue;

            await using var command = connection.CreateCommand();
            command.CommandText = trimmed;
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task RunPostgresScriptAsync(string connectionString, string script, CancellationToken ct)
    {
        // Npgsql's simple query protocol parses a full multi-statement script server-side, dollar
        // quoting included -- the script's DO $$ ... END $$; block (used for the conditional
        // DROP TABLEs) has semicolons inside it, so splitting on ';' the way the SQL Server script
        // is split on 'GO' would incorrectly break that block apart. Sending it whole is correct and
        // is exactly what tools like psql rely on for script execution.
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        await using var command = connection.CreateCommand();
        command.CommandText = script;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunMySqlStatementsAsync(string connectionString, string script, CancellationToken ct)
    {
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(ct);

        // Plain sequential DDL, no procedural blocks -- safe to split on ';' and run one statement
        // at a time, same approach as the SQL Server script's GO-based split. Strip full-line
        // comments first -- the vendored script has a commented-out example
        // ("# CREATE DATABASE ...;") whose semicolon would otherwise be misread as a real statement
        // terminator, splitting the file at the wrong point and sending an all-comment fragment to
        // MySQL as an empty statement (syntax error).
        var withoutComments = string.Join('\n', script
            .Split('\n')
            .Where(line => !line.TrimStart().StartsWith("--") && !line.TrimStart().StartsWith('#')));
        var statements = withoutComments.Split(';');
        foreach (var statement in statements)
        {
            var trimmed = statement.Trim();
            if (trimmed.Length == 0) continue;

            await using var command = connection.CreateCommand();
            command.CommandText = trimmed;
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
