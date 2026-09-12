namespace JobMaster.Sdk.Abstractions.Background;

/// <summary>
/// Manages concurrent job execution for a single bucket, providing a two-layer
/// queue: actively running slots and a bounded waiting queue.
/// </summary>
/// <typeparam name="T">The job value type stored in each queue item.</typeparam>
internal interface ITaskQueueControl<T>
{
    /// <summary>
    /// Scans all running slots, lazily frees any that have completed, faulted, or been
    /// cancelled, then promotes items from the waiting queue into the newly freed slots
    /// by calling <see cref="ITaskQueueItem{T}.Start"/> on each.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> if at least one waiting item was promoted and started;
    /// <see langword="false"/> if no items were started (waiting queue empty, all slots
    /// occupied, or the control is shutting down).
    /// </returns>
    bool StartQueuedTasksIfHasSlotAvailable();

    /// <summary>
    /// Returns the number of running slots that currently hold an active
    /// <see cref="ITaskQueueItem{T}"/>. Returns <c>0</c> while shutting down.
    /// </summary>
    int CountRunning();

    /// <summary>
    /// Returns the remaining capacity of the waiting queue
    /// (<c>WaitingQueueCapacity - waiting items</c>). This measures waiting queue
    /// space, <b>not</b> the number of free running slots. Returns <c>0</c> while
    /// shutting down.
    /// </summary>
    int CountAvailability();

    /// <summary>
    /// Returns the number of items currently sitting in the waiting queue.
    /// Returns <c>0</c> while shutting down.
    /// </summary>
    int CountWaiting();

    /// <summary>
    /// Returns <see langword="true"/> if an item with the given <paramref name="id"/>
    /// is currently tracked — either in the waiting queue or in a running slot.
    /// </summary>
    bool Contains(string id);

    /// <summary>
    /// Adds <paramref name="queueItem"/> to the waiting queue.
    /// </summary>
    /// <remarks>
    /// <para>
    /// If a <c>preEnqueueAction</c> was supplied at construction time it is invoked with
    /// the item's value before the item is queued. A <see langword="false"/> return value
    /// from the action causes <c>Enqueue</c> to reject the item.
    /// </para>
    /// <para>
    /// If the item's ID is already tracked (waiting or running), the call is treated as a
    /// no-op and returns <see langword="true"/> without invoking the action again.
    /// </para>
    /// </remarks>
    /// <returns>
    /// <see langword="true"/> on success or when the item is already tracked;
    /// <see langword="false"/> when the waiting queue is full, the pre-enqueue action
    /// rejects the item, or the control is shutting down.
    /// </returns>
    bool Enqueue(ITaskQueueItem<T> queueItem);

    /// <summary>
    /// Initiates a graceful shutdown: drains the waiting queue (returning the values of
    /// all drained items), then waits up to 5 seconds for any currently running tasks to
    /// finish before returning. All subsequent operations return empty/false results.
    /// </summary>
    /// <returns>
    /// The values of every item that was in the waiting queue at the time of shutdown.
    /// Running items are not included.
    /// </returns>
    Task<IList<T>> ShutdownAsync();

    /// <summary>
    /// Returns the configured <see cref="ITaskQueueItem{T}.Timeout"/> of each currently
    /// occupied running slot. Used by the engine to compute the postpone time span.
    /// </summary>
    IEnumerable<TimeSpan> GetRunningTimeouts();

    /// <summary>
    /// Returns a snapshot of all item IDs currently tracked by this control,
    /// covering both waiting and running items.
    /// </summary>
    IList<string> GetIds();
}
