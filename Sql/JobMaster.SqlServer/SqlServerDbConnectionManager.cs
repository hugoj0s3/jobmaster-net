using System.Data;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.SqlBase.Connections;
using Microsoft.Data.SqlClient;

namespace JobMaster.SqlServer;

internal class SqlServerDbConnectionManager : DbConnectionManager, IDbConnectionManager
{
    public override IDbConnection Open(
        string connectionString, 
        JobMasterConfigDictionary? additionalConnConfig = null,
        ReadIsolationLevel isolationLevel = ReadIsolationLevel.Consistent)
    {
        var conn = new SqlConnection(connectionString);
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SET TRANSACTION ISOLATION LEVEL {IsolationLevelToSql(isolationLevel)};";
        cmd.ExecuteNonQuery();

        return conn;
    }

    public override async Task<IDbConnection> OpenAsync(
        string connectionString, 
        JobMasterConfigDictionary? additionalConnConfig = null,
        ReadIsolationLevel isolationLevel = ReadIsolationLevel.Consistent)
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SET TRANSACTION ISOLATION LEVEL {IsolationLevelToSql(isolationLevel)};";
        await cmd.ExecuteNonQueryAsync();

        return conn;
    }
    
    private static string IsolationLevelToSql(ReadIsolationLevel isolationLevel)
    {
        return isolationLevel switch
        {
            ReadIsolationLevel.FastSync => "READ UNCOMMITTED",
            _ => "READ COMMITTED"
        };
    }
}
