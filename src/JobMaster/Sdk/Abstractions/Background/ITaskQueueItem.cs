namespace JobMaster.Sdk.Abstractions.Background;

/// <summary>
/// Represents a single job item managed by <see cref="ITaskQueueControl{T}"/>, wrapping
/// both the job value and the underlying <see cref="System.Threading.Tasks.Task"/> that
/// executes it.
/// </summary>
/// <typeparam name="T">The job value type.</typeparam>
internal interface ITaskQueueItem<T> : IDisposable
{
    /// <summary>
    /// Unique identifier for this item, used for deduplication and lookup across
    /// both the waiting queue and running slots.
    /// </summary>
    string Id { get; }

    /// <summary>
    /// The job value carried by this item.
    /// </summary>
    T Value { get; }

    /// <summary>
    /// The underlying task. Its completion state (<see cref="Task.IsCompleted"/>,
    /// <see cref="Task.IsFaulted"/>, <see cref="Task.IsCanceled"/>) is polled lazily
    /// by <see cref="ITaskQueueControl{T}.StartQueuedTasksIfHasSlotAvailable"/> to
    /// detect when a running slot can be freed.
    /// </summary>
    Task Task { get; }

    /// <summary>
    /// Maximum allowed execution duration for this item, as configured by the job
    /// definition. Used by <see cref="ITaskQueueControl{T}.GetRunningTimeouts"/> to
    /// feed the engine's postpone-time calculation.
    /// </summary>
    TimeSpan Timeout { get; }

    /// <summary>
    /// UTC timestamp recorded when the item entered the queue (before <see cref="Start"/> is called).
    /// </summary>
    DateTime EnqueuedAt { get; }

    /// <summary>
    /// UTC timestamp recorded when <see cref="Start"/> was called, or <see langword="null"/>
    /// if the item has not yet been promoted to a running slot.
    /// </summary>
    DateTime? StartedAt { get; }

    /// <summary>
    /// Token source used to request cooperative cancellation of the running job.
    /// </summary>
    CancellationTokenSource CancellationTokenSource { get; }

    /// <summary>
    /// Returns <see langword="true"/> if the item has been running longer than
    /// its configured <see cref="Timeout"/>.
    /// </summary>
    bool IsTimedOut();

    /// <summary>
    /// Returns the time elapsed since the item entered the queue.
    /// </summary>
    TimeSpan GetElapsedTime();

    /// <summary>
    /// Requests immediate cancellation of the running job by signalling
    /// <see cref="CancellationTokenSource"/>.
    /// </summary>
    void Abort();

    /// <summary>
    /// Promotes the item from the waiting queue into a running slot.
    /// Implementations should begin executing the job and set <see cref="StartedAt"/>.
    /// </summary>
    void Start();
}
