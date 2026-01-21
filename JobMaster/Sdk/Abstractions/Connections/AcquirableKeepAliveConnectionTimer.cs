using System.ComponentModel;
using JobMaster.Internals;

namespace JobMaster.Sdk.Abstractions.Connections;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AcquirableKeepAliveConnectionTimer<T> : IDisposable
{
    private bool isDisposed = false;

    private readonly Timer? KeepAliveTimer;
    private readonly Timer? IdleTimeoutTimer;
    
    private DateTime lastUsedUtc;
    
    protected AcquirableKeepAliveConnectionTimer(
        string connectionId,
        int gate,
        SemaphoreSlim semaphore,
        TimeSpan idleTimeout,
        TimeSpan? keepAliveInterval)
    {
        ConnectionId = connectionId;
        Gate = gate;
        this.Semaphore = semaphore;
        this.IdleTimeout = idleTimeout;

        if (keepAliveInterval.HasValue)
        {
            KeepAliveTimer = new Timer(OnKeepAliveTimer, null, keepAliveInterval.Value, keepAliveInterval.Value);
        }
        
        IdleTimeoutTimer = new Timer(CheckIdleTimeout, null, idleTimeout, idleTimeout);
        
        lastUsedUtc = DateTime.UtcNow;
    }


    public object Mutex { get; } = new object();
    
    public string ConnectionId { get; }
    public int Gate { get; }
    
    public TimeSpan IdleTimeout { get; private set; }
    
    public SemaphoreSlim Semaphore { get; }


    public void NotifyUsage()
    {
        if (isDisposed) return;
        lastUsedUtc = DateTime.UtcNow;
    }
    
    public void UpdateIdleTimeout(TimeSpan timeSpan)
    {
        if (isDisposed) return;
        IdleTimeout = timeSpan;
        
        lock (Mutex)
        {
            IdleTimeoutTimer?.Change(timeSpan, timeSpan);
        }
    }
    
    public abstract void Close();
    public abstract T Connection { get; }
    
    protected abstract void SendKeepAliveSignal();
    
    private void OnKeepAliveTimer(object? state)
    {
        if (isDisposed) return;
        SendKeepAliveSignal();
    }
    
    private void CheckIdleTimeout(object? _)
    {
        if (isDisposed) return;
        var idleFor = DateTime.UtcNow - lastUsedUtc;
        if (idleFor < IdleTimeout) return;

        var acquired = false;
        try
        {
            acquired = Semaphore.Wait(0);
            if (!acquired) return; // in-use; skip this cycle
            Close();
        }
        finally
        {
            if (acquired) Semaphore.Release();
        }
    }
    
    public virtual void Dispose()
    {
        if (isDisposed) return;
        isDisposed = true;
        
        if (Connection is IDisposable disposable)
        {
            disposable.SafeDispose();
        }
        
        Semaphore.SafeDispose();
        
        KeepAliveTimer?.SafeDispose();
        IdleTimeoutTimer?.SafeDispose();
    }
}