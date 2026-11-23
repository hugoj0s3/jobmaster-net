namespace JobMaster.Sdk.Background.Cancellation;

/// <summary>
/// One level of the cancellation tree (runtime, worker, runner or job): a stop token and an abort token, each linked
/// to the same token of the parent. <see cref="StopToken"/> means take no new work, while what is running continues;
/// <see cref="AbortToken"/> means cancel what is running. Aborting also stops. Stopping or aborting one reaches
/// its children, never its parent. Created only by <see cref="JobMasterStopControlFactory"/>.
/// </summary>
internal sealed class StopControl : IDisposable
{
    // Null only on None.
    private CancellationTokenSource? abort;
    private CancellationTokenSource? stop;

    private StopControl()
    {
        IsNone = true;
    }

    /// <summary>
    /// No stop control, like <see cref="CancellationToken.None"/>: it never stops or aborts. An owner's stop control
    /// until it takes its own.
    /// </summary>
    public static StopControl None { get; } = new StopControl();

    public static StopControl CreateLinked(StopControl parent)
    {
        if (parent.IsNone)
        {
            throw new ArgumentException("A stop control can't be linked to None: its parent's isn't set.", nameof(parent));
        }

        var obj = new StopControl();
        obj.abort = CancellationTokenSource.CreateLinkedTokenSource(parent.AbortToken);
        obj.stop = CancellationTokenSource.CreateLinkedTokenSource(parent.StopToken, obj.abort.Token);
        obj.IsRoot = false;
        obj.IsNone = false;

        return obj;
    }
    
    public static StopControl Create()
    {
        var obj = new StopControl();
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
            throw new InvalidOperationException("StopControl.None can't be stopped.");
        }

        if (!stop.IsCancellationRequested)
        {
            stop.Cancel();
        }
    }

    /// <summary>
    /// <see cref="Stop"/> with the token's callbacks run asynchronously (where the framework can), so a caller isn't
    /// held by them: a runner stopping itself must not run its own loop's continuations inline.
    /// </summary>
    public async Task StopAsync()
    {
        if (stop == null)
        {
            throw new InvalidOperationException("StopControl.None can't be stopped.");
        }

        if (!stop.IsCancellationRequested)
        {
#if NETSTANDARD2_0
            stop.Cancel();
            await Task.CompletedTask;
#else
            await stop.CancelAsync();
#endif
        }
    }

    public void Abort()
    {
        if (abort == null)
        {
            throw new InvalidOperationException("StopControl.None can't be aborted.");
        }

        if (!abort.IsCancellationRequested)
        {
            abort.Cancel();
        }
    }
    
    public void Dispose()
    {
        // The stop source is linked to the abort source: it goes first.
        stop?.Dispose();
        abort?.Dispose();
    }
}
