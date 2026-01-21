using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Connections;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IAcquirableKeepAliveConnection<T> : IDisposable
{
    string ConnectionId { get; }
    T? Connection { get; }
    
    int Gate { get; }
    
    TimeSpan IdleTimeTimeout { get; }
    
    bool IsAcquired { get; }
}