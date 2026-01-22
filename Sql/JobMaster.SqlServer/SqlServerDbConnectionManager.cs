using System.Data;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.SqlBase.Connections;
using Microsoft.Data.SqlClient;

namespace JobMaster.SqlServer;

internal class SqlServerDbConnectionManager : DbConnectionManager, IDbConnectionManager
{

    public override IDbConnection Open(string connectionString, JobMasterConfigDictionary? additionalConnConfig = null)
    {
        var conn = new SqlConnection(connectionString);
        conn.Open();
        return conn;
    }

    public override async Task<IDbConnection> OpenAsync(string connectionString, JobMasterConfigDictionary? additionalConnConfig = null)
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
