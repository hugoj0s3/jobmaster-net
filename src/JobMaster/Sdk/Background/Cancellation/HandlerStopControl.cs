namespace JobMaster.Sdk.Background.Cancellation;

/// <summary>
/// A job handler's token: cancelled when the job's stop control aborts, after the job's timeout, or when the job's
/// stop control stops and the immediate graceful stop period then runs out. Created by
/// <see cref="JobMasterStopControlFactory.ForHandler"/> right before the handler; the caller disposes it.
/// </summary>
internal sealed class HandlerStopControl : IDisposable
{
    // Not linked to anything: it starts counting only when the job's stop control stops.
    private readonly CancellationTokenSource immediateGracefulStop = new();
    private readonly CancellationTokenSource handler;
    private readonly CancellationTokenRegistration stopRegistration;

    internal HandlerStopControl(
        StopControl taskItemStopControl, 
        TimeSpan timeout, 
        TimeSpan immediateGracefulStopPeriod)
    {
        // One source for abort, timeout and the stop's grace: disposing it also stops its timer.
        handler = CancellationTokenSource.CreateLinkedTokenSource(taskItemStopControl.AbortToken, immediateGracefulStop.Token);
        handler.CancelAfter(timeout);

        // If the job's stop control already stopped, the countdown starts now.
        stopRegistration = taskItemStopControl.StopToken.Register(() => immediateGracefulStop.CancelAfter(immediateGracefulStopPeriod));
    }

    public CancellationToken Token => handler.Token;

    public bool IsCancellationRequested => handler.IsCancellationRequested;

    /// <summary>The job's stop control stopped and the immediate graceful stop period ran out: interrupted.</summary>
    public bool IsImmediateGracefulStopOver() => immediateGracefulStop.IsCancellationRequested;

    public void Dispose()
    {
        // First: once removed, no stop can schedule CancelAfter on a disposed source.
        stopRegistration.Dispose();
        handler.Dispose();
        immediateGracefulStop.Dispose();
    }
}
