using System;
using System.Collections.Concurrent;
using System.Data;
using System.Threading;
using JobMaster.Sql.Connections;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Services.Master;
using Npgsql;

namespace JobMaster.Postgres;

internal class PostgresDbConnectionManager : DbConnectionManager, IDbConnectionManager
{
    public override IDbConnection Open(string connectionString, JobMasterConfigDictionary? additionalConnConfig = null)
    {
        var conn = new NpgsqlConnection(connectionString);
        conn.Open();
        
        return conn;
       
    }

    public override async Task<IDbConnection> OpenAsync(string connectionString, JobMasterConfigDictionary? additionalConnConfig = null)
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        
        return conn;
       
    }
}