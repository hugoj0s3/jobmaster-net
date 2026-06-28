using System.Data;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.SqlBase.Connections;
using MySqlConnector;

namespace JobMaster.MySql;

internal class MySqlDbConnectionManager : DbConnectionManager, IDbConnectionManager
{
    public override IDbConnection Open(
        string connectionString,
        JobMasterConfigDictionary? additionalConnConfig = null)
    {
        var conn = new MySqlConnection(connectionString);
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;";
        cmd.ExecuteNonQuery();

        return conn;
    }

    public override async Task<IDbConnection> OpenAsync(
        string connectionString,
        JobMasterConfigDictionary? additionalConnConfig = null)
    {
        var conn = new MySqlConnection(connectionString);
        try
        {
            await conn.OpenAsync();

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;";
            await cmd.ExecuteNonQueryAsync();

            return conn;
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }
}
