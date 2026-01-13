using System.Data;
using Dapper;
using JobMaster.Sdk.Connections;

namespace JobMaster.Sql.Connections;

public class DbConnectionKeepAliveConnectionTimer : AcquirableKeepAliveConnectionTimer<IDbConnection>
{
    public DbConnectionKeepAliveConnectionTimer(
        string connectionId,
        int gate,
        IDbConnection connection,
        SemaphoreSlim semaphore,
        TimeSpan idleTimeout,
        TimeSpan? keepAliveInterval) : base(connectionId, gate, semaphore, idleTimeout, keepAliveInterval)
    {
        Connection = connection;
    }

    public override void Close()
    {
        Connection.Close();
    }

    public override IDbConnection Connection { get; }
    protected override void SendKeepAliveSignal()
    {
        try 
        { 
            Connection.Execute("SELECT 1"); 
        }
        catch 
        { 
            try { Connection.Close(); } catch { }
        }
    }
}