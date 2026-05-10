namespace JobMaster.Sdk.Abstractions.Background;

internal interface ITaskQueueControl<T>
{
    bool StartQueuedTasksIfHasSlotAvailable();
    int CountRunning();
    int CountAvailability();
    int CountWaiting();
    bool Contains(string id);
    bool Enqueue(ITaskQueueItem<T> queueItem);
    Task<IList<T>> ShutdownAsync();
    IEnumerable<TimeSpan> GetRunningTimeouts();
    IList<string> GetIds();
}
