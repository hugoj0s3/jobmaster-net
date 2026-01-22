using System;
using System.Data;
using System.Diagnostics;
using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.SqlBase.Connections;
using MySqlConnector;

namespace JobMaster.MySql;

internal class MySqlDbConnectionManager : DbConnectionManager, IDbConnectionManager
{
    public override IDbConnection Open(string connectionString, JobMasterConfigDictionary? additionalConnConfig = null)
    {
        var conn = new MySqlConnection(connectionString);
        conn.Open(); 
        
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;";
            cmd.ExecuteNonQuery();
        }
        
        return conn;
    }

    public override async Task<IDbConnection> OpenAsync(string connectionString, JobMasterConfigDictionary? additionalConnConfig = null)
    {
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        
        await conn.ExecuteAsync("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;");
        
        return conn;
    }
}