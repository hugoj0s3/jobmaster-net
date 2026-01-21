using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Background;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface ITaskQueueItem<T> : IDisposable
{
    string Id { get; }
    T Value { get; }
    Task Task { get; }
    TimeSpan Timeout { get; }
    DateTime EnqueuedAt { get; }
    DateTime? StartedAt { get; }
    CancellationTokenSource CancellationTokenSource { get; }
    
    bool IsTimedOut();
    TimeSpan GetElapsedTime();
    void Abort();
    void Start();
}
