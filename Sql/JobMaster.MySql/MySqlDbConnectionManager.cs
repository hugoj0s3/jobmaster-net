using System;
using System.Data;
using System.Diagnostics;
using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.SqlBase.Connections;
using MySqlConnector;

namespace JobMaster.MySql;

internal class MySqlDbConnectionManager : DbConnectionManager, IDbConnectionManager
{
    public override IDbConnection Open(
        string connectionString, 
        JobMasterConfigDictionary? additionalConnConfig = null,
        ReadIsolationLevel isolationLevel = ReadIsolationLevel.Consistent)
    {
        var conn = new MySqlConnection(connectionString);
        conn.Open(); 
        
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SET SESSION TRANSACTION ISOLATION LEVEL {IsolationLevelToSql(isolationLevel)};";
        cmd.ExecuteNonQuery();
        
        return conn;
    }

    public override async Task<IDbConnection> OpenAsync(
        string connectionString, 
        JobMasterConfigDictionary? additionalConnConfig = null,
        ReadIsolationLevel isolationLevel = ReadIsolationLevel.Consistent)
    {
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SET SESSION TRANSACTION ISOLATION LEVEL {IsolationLevelToSql(isolationLevel)};";
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