using JobMaster.Sdk.Contracts.Background;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

/// <summary>
/// Container for managing tasks with timeout tracking and cancellation support.
/// </summary>
/// <typeparam name="T">The type of value associated with the task</typeparam>
public class TaskQueueItem<T> : ITaskQueueItem<T>, IDisposable
{
    private readonly CancellationTokenSource cancellationTokenSource;
    private readonly Func<CancellationToken, Task> runAsync;
    private readonly Action? afterAction;
    private bool started;

    public TaskQueueItem(string id, T value, TimeSpan timeout, Func<CancellationToken, Task> action, Action? afterAction = null)
    {
        Id = id;
        Value = value;
        runAsync = action;
        Task = Task.CompletedTask; // initialized so status checks work pre-start
        Timeout = timeout;
        EnqueuedAt = DateTime.UtcNow;
        cancellationTokenSource = new CancellationTokenSource();
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
    /// Cancellation token source for aborting the task
    /// </summary>
    public CancellationTokenSource CancellationTokenSource => cancellationTokenSource;
    
    /// <summary>
    /// Checks if the task has exceeded its timeout duration
    /// </summary>
    public bool IsTimedOut() => StartedAt.HasValue && DateTime.UtcNow - StartedAt.Value > Timeout;
    
    /// <summary>
    /// Gets the elapsed time since the task started
    /// </summary>
    public TimeSpan GetElapsedTime() => DateTime.UtcNow - EnqueuedAt;

    /// <summary>
    /// Aborts the task by cancelling its cancellation token
    /// </summary>
    public void Abort()
    {
        if (!cancellationTokenSource.IsCancellationRequested)
        {
            cancellationTokenSource.Cancel();
        }
    }
    
    public void Start()
    {
        if (started) return;
        started = true;
        StartedAt = DateTime.UtcNow;
        if (Timeout > TimeSpan.Zero)
        {
            try { cancellationTokenSource.CancelAfter(Timeout); } catch { /* ignore */ }
        }
        Task = runAsync(cancellationTokenSource.Token);

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
        
        cancellationTokenSource?.Dispose();
        Task.Dispose();
    }
}