using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;

namespace JobMaster.Sdk.Background.Cancellation;

/// <summary>
/// The one place JobMaster creates cancellation sources, so the tree (runtime → worker → runner → job) and every
/// source deliberately left out of it are visible in this file. Each owner's stop control is created from its
/// parent's; the owner takes it itself (the runtime in its constructor, a worker or runner when it starts). Owned by
/// the runtime.
/// <para>Terms:</para>
/// <list type="bullet">
///   <item><b>Graceful stop</b> (<c>RequestGracefulStop</c>, e.g. a signal): the worker takes no new jobs and runs the
///   ones it has for its period (<see cref="JobMasterConstants.DefaultGracefulStopPeriod"/>, at least
///   <see cref="JobMasterConstants.MinGracefulStopPeriod"/>), then stops immediately. No stop control is involved.</item>
///   <item><b>Immediate stop</b> (<c>StopImmediatelyAsync</c>: SIGTERM, process exit, end of a graceful stop):
///   <see cref="StopControl.Stop"/>, so no new work starts; a running job gets
///   <see cref="JobMasterConstants.ImmediateGracefulStopPeriod"/> to finish.</item>
///   <item><b>Abort</b>: <see cref="StopControl.Abort"/>, which cancels what is still running; the job goes back
///   to master.</item>
/// </list>
/// </summary>
internal class JobMasterStopControlFactory
{
    /// <summary>The root of the tree: the runtime's stop control.</summary>
    public StopControl ForRoot()
    {
        return StopControl.Create();
    }

    /// <summary>A worker's stop control, stopped and aborted with its runtime's.</summary>
    public StopControl ForWorker(IJobMasterBackgroundAgentWorker worker)
    {
        var runtime = worker.Runtime ?? throw new InvalidOperationException($"Worker {worker.AgentWorkerId} has no runtime to link its stop control to.");
        return StopControl.CreateLinked(runtime.StopControl);
    }

    /// <summary>A runner's stop control, stopped and aborted with its worker's.</summary>
    public StopControl ForRunner(IJobMasterBackgroundAgentWorker worker)
    {
        return StopControl.CreateLinked(worker.StopControl);
    }

    /// <summary>A running job's stop control, stopped and aborted with its runner's.</summary>
    public StopControl ForTaskItem(StopControl runnerStopControl)
    {
       var stopControl = StopControl.CreateLinked(runnerStopControl);
       return stopControl;
    }

    /// <summary>
    /// A job handler's token: cancelled when the job's stop control aborts (itself or through a parent) or after the
    /// job's <paramref name="timeout"/>. A stop doesn't cancel it directly: the job gets
    /// <see cref="JobMasterConstants.ImmediateGracefulStopPeriod"/> to finish first (then interrupted, back on master).
    /// Create it right before the handler; the caller disposes it.
    /// </summary>
    public HandlerStopControl ForHandler(StopControl taskItemStopControl, TimeSpan timeout)
    {
        return new HandlerStopControl(taskItemStopControl, timeout, JobMasterConstants.ImmediateGracefulStopPeriod);
    }

    /// <summary>
    /// A retry interceptor's token: cancelled after <paramref name="limit"/> or when the job's stop control aborts, so
    /// an aborting worker doesn't wait on it. The caller disposes it.
    /// </summary>
    public CancellationTokenSource ForRetryInterceptor(StopControl taskItemStopControl, TimeSpan limit)
    {
        var interceptor = CancellationTokenSource.CreateLinkedTokenSource(taskItemStopControl.AbortToken);
        interceptor.CancelAfter(limit);
        return interceptor;
    }

    /// <summary>
    /// A single operation's token: cancelled with <paramref name="parent"/> (the caller's token, if it has one) or
    /// after <paramref name="limit"/>, whichever comes first. The caller disposes it.
    /// </summary>
    public CancellationTokenSource ForOperation(CancellationToken parent, TimeSpan limit)
    {
        var operation = CancellationTokenSource.CreateLinkedTokenSource(parent);
        operation.CancelAfter(limit);
        return operation;
    }

    /// <summary>
    /// Cancelled only after <paramref name="limit"/>, on purpose not linked to any stop control: for work that must
    /// still finish while stopping or after an abort (flushing to master, handing buckets over, an ack, the write that
    /// puts an interrupted job back on master). Linked to a stop control, it would be cancelled the moment it starts.
    /// The caller disposes it.
    /// </summary>
    public CancellationTokenSource Cleanup(TimeSpan limit)
    {
        return new CancellationTokenSource(limit);
    }
}
