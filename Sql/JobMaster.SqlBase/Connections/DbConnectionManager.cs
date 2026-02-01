using System.Collections.Concurrent;
using System.Data;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Connections;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.SqlBase.Connections;

internal interface IDbConnectionManager : IAcquirableKeepAliveConnectionManager<IDbConnection>
{
    IDbConnection Open(
        string connectionString, 
        JobMasterConfigDictionary? additionalConnConfig = null,
        ReadIsolationLevel isolationLevel = ReadIsolationLevel.Consistent);
    Task<IDbConnection> OpenAsync(
        string connectionString,  
        JobMasterConfigDictionary? additionalConnConfig = null,
        ReadIsolationLevel isolationLevel = ReadIsolationLevel.Consistent);
}

internal abstract class DbConnectionManager : IDbConnectionManager, IDisposable
{
    private readonly ConcurrentDictionary<string, AcquirableKeepAliveConnectionTimer<IDbConnection>> keepAliveConnections
        = new ConcurrentDictionary<string, AcquirableKeepAliveConnectionTimer<IDbConnection>>();

    public abstract IDbConnection Open(
        string connectionString,  
        JobMasterConfigDictionary? additionalConnConfig = null,
        ReadIsolationLevel isolationLevel = ReadIsolationLevel.Consistent);
    public abstract Task<IDbConnection> OpenAsync(
        string connectionString,  
        JobMasterConfigDictionary? additionalConnConfig = null,
        ReadIsolationLevel isolationLevel = ReadIsolationLevel.Consistent);

    /// <summary>
    /// Acquires a keep-alive database connection from the pool, ensuring concurrency control through semaphores.
    /// </summary>
    /// <param name="connectionId">The unique identifier for the connection resource.</param>
    /// <param name="idleTimeTimeout">The duration after which an idle connection should be disposed.</param>
    /// <param name="connectionString">The database connection string.</param>
    /// <param name="additionalConnConfig">Optional dictionary for database-specific configurations.</param>
    /// <param name="maxGates">Number of concurrent connection 'gates' available for load distribution.</param>
    /// <returns>An acquirable connection wrapper containing the DB connection and its acquisition status.</returns>
    public IAcquirableKeepAliveConnection<IDbConnection> AcquireConnection(
        string connectionId,
        TimeSpan idleTimeTimeout,
        string connectionString,
        JobMasterConfigDictionary? additionalConnConfig = null,
        int maxGates = 1)
    {
        // Randomly select a gate to distribute load across multiple keep-alive slots
        var gate = JobMasterRandomUtil.GetInt(0, maxGates);
        var fullConnectionId = $"{connectionId}:{gate}";

        // Retrieve the existing connection timer or create a new one if it doesn't exist or is closed
        var dbKeepAliveConnectionTimer = keepAliveConnections.AddOrUpdate(fullConnectionId,
            s => { return AcquirableKeepAliveConnectionTimer(idleTimeTimeout, connectionString, additionalConnConfig, fullConnectionId, gate); },
            (s, existing) =>
            {
                // If the existing connection is no longer open, dispose it and create a fresh one
                if (existing.Connection.State != ConnectionState.Open)
                {
                    existing.SafeDispose();
                    return AcquirableKeepAliveConnectionTimer(idleTimeTimeout, connectionString, additionalConnConfig, fullConnectionId, gate);
                }
                else
                {
                    // Update last-used timestamp to prevent premature idle disposal
                    existing.NotifyUsage();
                    return existing;
                }
            });

        // Bounded Timeout: Wait for the semaphore to be available.
        // We use a fixed timeout (5s) to implement Load Shedding. If the DB is too busy, 
        // we fail fast rather than hanging the entire Agent runtime.
        var isAcquired = dbKeepAliveConnectionTimer.Semaphore.Wait(TimeSpan.FromSeconds(5));
        if (!isAcquired)
        {
            return new AcquirableKeepAliveConnection<IDbConnection>(connectionId, null, idleTimeTimeout, gate, false, null);
        }

        try
        {
            // Ensure the connection is actually Open before handing it to the repository
            if (dbKeepAliveConnectionTimer.Connection.State != ConnectionState.Open)
            {
                dbKeepAliveConnectionTimer.Connection.Open();
            }
        }
        catch (Exception)
        {
            // Critical: If opening fails, we MUST release the semaphore immediately
            // so other threads aren't blocked by this failed slot.
            dbKeepAliveConnectionTimer.Semaphore.Release();
            throw;
        }

        // Return the wrapper. The Dispose() method of this object will release the semaphore.
        return new AcquirableKeepAliveConnection<IDbConnection>(
            connectionId,
            dbKeepAliveConnectionTimer.Connection,
            idleTimeTimeout,
            gate,
            true,
            dbKeepAliveConnectionTimer.Semaphore);
    }

    protected virtual TimeSpan? KeepAliveInterval()
    {
        return null;
    }

    private AcquirableKeepAliveConnectionTimer<IDbConnection> AcquirableKeepAliveConnectionTimer(TimeSpan idleTimeTimeout, string connectionString, JobMasterConfigDictionary? additionalConnConfig,
        string connectionId, int gate)
    {
        var connection = Open(connectionString, additionalConnConfig);
        var semaphore = new SemaphoreSlim(1);
        return new DbConnectionKeepAliveConnectionTimer(connectionId, gate, connection, semaphore, idleTimeTimeout, KeepAliveInterval());
    }

    public void Dispose()
    {
        foreach (var entry in keepAliveConnections)
        {
            entry.Value.SafeDispose();
        }
    }
}