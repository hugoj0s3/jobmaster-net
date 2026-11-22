namespace JobMaster.Sdk.Background.Cancellation;

/// <summary>
/// One level of the cancellation tree (runtime, worker, runner or job): a stop token and an abort token, each linked
/// to the same token of the parent. <see cref="StopToken"/> means take no new work, while what is running continues;
/// <see cref="AbortToken"/> means cancel what is running. Aborting also stops. Stopping or aborting a scope reaches
/// its children, never its parent. Created only by <see cref="JobMasterStopScopeFactory"/>.
/// </summary>
internal sealed class StopScope : IDisposable
{
    // Null only on None.
    private CancellationTokenSource? abort;
    private CancellationTokenSource? stop;
    private CancellationTokenSource? timeout;
    private CancellationTokenSource? handler;
    
    private StopScope()
    {
        IsNone = true;
    }

    /// <summary>
    /// No scope, like <see cref="CancellationToken.None"/>: it never stops or aborts. An owner's scope until the
    /// factory assigns its own.
    /// </summary>
    public static StopScope None { get; } = new StopScope();

    public static StopScope CreateLinked(StopScope parent)
    {
        if (parent.IsNone)
        {
            throw new ArgumentException("A stop scope can't be linked to None: its parent's scope isn't set.", nameof(parent));
        }

        var obj = new StopScope();
        obj.abort = CancellationTokenSource.CreateLinkedTokenSource(parent.AbortToken);
        obj.stop = CancellationTokenSource.CreateLinkedTokenSource(parent.StopToken, obj.abort.Token);
        obj.IsRoot = false;
        obj.IsNone = false;

        return obj;
    }
    
    /// <summary>
    /// Starts the job's timeout now and returns the handler's token: cancelled when the scope aborts or the timeout
    /// passes, never by a stop (a stopping worker lets running handlers finish). Call it right before the handler.
    /// </summary>
    public CancellationToken StartHandlerTimeout(TimeSpan limit)
    {
        if (abort == null)
        {
            throw new InvalidOperationException("StopScope.None has no handler.");
        }

        if (handler != null)
        {
            throw new InvalidOperationException("The handler's timeout already started.");
        }

        // Not linked to anything: it fires only when the time passes, so IsTimedOut() means exactly that.
        timeout = new CancellationTokenSource(limit);
        handler = CancellationTokenSource.CreateLinkedTokenSource(abort.Token, timeout.Token);
        return handler.Token;
    }
    
    public static StopScope Create()
    {
        var obj = new StopScope();
        obj.abort = new CancellationTokenSource();
        obj.stop = CancellationTokenSource.CreateLinkedTokenSource(obj.abort.Token);
        obj.IsNone = false;
        obj.IsRoot = true;
        
        return obj;
    }

    public CancellationToken StopToken => stop?.Token ?? CancellationToken.None;

    public CancellationToken AbortToken => abort?.Token ?? CancellationToken.None;

    public bool IsRoot { get; private set; }

    public bool IsNone { get; private set; }

    public bool IsStopping() => stop?.IsCancellationRequested == true;

    public bool IsAborted() => abort?.IsCancellationRequested == true;

    public void Stop()
    {
        if (stop == null)
        {
            throw new InvalidOperationException("StopScope.None can't be stopped.");
        }

        if (!stop.IsCancellationRequested)
        {
            stop.Cancel();
        }
    }

    public void Abort()
    {
        if (abort == null)
        {
            throw new InvalidOperationException("StopScope.None can't be aborted.");
        }

        if (!abort.IsCancellationRequested)
        {
            abort.Cancel();
        }
    }
    
    public bool IsTimedOut() => timeout?.IsCancellationRequested == true;
    
    public void Dispose()
    {
        // The stop source is linked to the abort source: it goes first.
        stop?.Dispose();
        abort?.Dispose();
        timeout?.Dispose();
        handler?.Dispose();
    }
}
