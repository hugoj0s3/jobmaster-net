using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Background.Cancellation;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

/// <summary>
/// Container for managing tasks with timeout tracking and cancellation support.
/// </summary>
/// <typeparam name="T">The type of value associated with the task</typeparam>
internal class TaskQueueItem<T> : ITaskQueueItem<T>, IDisposable
{
    private readonly Func<CancellationToken, Task> runAsync;
    private readonly Action? afterAction;
    private bool started;

    public TaskQueueItem(
        StopControl stopControl,
        string id, 
        T value, 
        TimeSpan timeout, 
        Func<CancellationToken, Task> action,
        Action? afterAction = null)
    {
        Id = id;
        Value = value;
        StopControl = stopControl;
        runAsync = action;
        Task = Task.CompletedTask; // initialized so status checks work pre-start
        Timeout = timeout;
        EnqueuedAt = DateTime.UtcNow;
        this.afterAction = afterAction;
    }
    
    public string Id { get; set; }
    
    public T Value { get; set; }
    public Task Task { get; private set; }
    
    /// <summary>
    /// The timeout duration for this task
    /// </summary>
    public TimeSpan Timeout { get; }
    
    /// <summary>
    /// When the task was started
    /// </summary>
    public DateTime EnqueuedAt { get; }
    
    public DateTime? StartedAt { get; set; }
    
    /// <summary>
    /// The job's stop control, a child of its runner's: aborted by <see cref="Abort"/> or with the runner, worker or runtime.
    /// It has no timer: a job's timeout is the handler's own.
    /// </summary>
    public StopControl StopControl { get; }
    
    /// <summary>
    /// Checks if the task has exceeded its timeout duration
    /// </summary>
    public bool IsTimedOut() => StartedAt.HasValue && DateTime.UtcNow - StartedAt.Value > Timeout;
    
    /// <summary>
    /// Gets the elapsed time since the task started
    /// </summary>
    public TimeSpan GetElapsedTime() => DateTime.UtcNow - EnqueuedAt;

    /// <summary>
    /// Aborts the task through its stop control's abort token.
    /// </summary>
    public void Abort()
    {
        StopControl.Abort();
    }
    
    public void Start()
    {
        if (started) return;
        started = true;
        StartedAt = DateTime.UtcNow;
        // The abort token, like the item's own source before: a stop lets the job run on.
        Task = runAsync(StopControl.AbortToken);

        if (afterAction != null)
        {
            Task.ContinueWith(t => afterAction.Invoke());
        } 
    }

    public void Dispose()
    {
        if (Value is IDisposable disposable)
        {
            disposable.Dispose();
        }
        
        // Not Task.Dispose(): it throws on a task still running (shutdown disposes those), and a task needs no
        // disposing unless its wait handle was used.
        StopControl.Dispose();
    }
}