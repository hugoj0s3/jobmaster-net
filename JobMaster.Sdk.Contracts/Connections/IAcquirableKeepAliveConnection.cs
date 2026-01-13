namespace JobMaster.Sdk.Contracts.Connections;

public interface IAcquirableKeepAliveConnection<T> : IDisposable
{
    string ConnectionId { get; }
    T? Connection { get; }
    
    int Gate { get; }
    
    TimeSpan IdleTimeTimeout { get; }
    
    bool IsAcquired { get; }
}