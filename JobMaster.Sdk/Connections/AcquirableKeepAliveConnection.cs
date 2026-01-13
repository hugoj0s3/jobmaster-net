using JobMaster.Sdk.Contracts.Connections;

namespace JobMaster.Sdk.Connections;

public class AcquirableKeepAliveConnection<T> : IAcquirableKeepAliveConnection<T>
{
    private readonly SemaphoreSlim? semaphoreToRelease;
    private bool isDisposed = false; 
    
    public AcquirableKeepAliveConnection(
        string connectionId,
        T? connection,
        TimeSpan idleTimeTimeout,
        int gate,
        bool isAcquired,
        SemaphoreSlim? semaphoreToRelease)
    {
        this.semaphoreToRelease = semaphoreToRelease;
        this.IdleTimeTimeout = idleTimeTimeout;
        ConnectionId = connectionId;
        Connection = connection;
        Gate = gate;
        IsAcquired = isAcquired;
    }
    
    public string ConnectionId { get; }
    public T? Connection { get; }
    public int Gate { get; }
    public TimeSpan IdleTimeTimeout { get; }
    public bool IsAcquired { get; }

    public virtual void Dispose()
    {
        if (isDisposed) return;
        isDisposed = true;
        
        semaphoreToRelease?.Release();
    }
}
