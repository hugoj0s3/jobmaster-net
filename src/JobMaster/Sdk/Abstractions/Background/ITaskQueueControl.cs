using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Background;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface ITaskQueueControl<T>
{
    bool StartQueuedTasksIfHasSlotAvailable();
    int CountRunning();
    int CountAvailability();
    int CountWaiting();
    bool Contains(string id);
    Task<bool> EnqueueAsync(ITaskQueueItem<T> queueItem);
    int AbortTimeoutTasks();
    Task<IList<T>> ShutdownAsync();
}
