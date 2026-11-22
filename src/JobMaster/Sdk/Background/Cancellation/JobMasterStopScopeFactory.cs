using JobMaster.Sdk.Abstractions.Background;

namespace JobMaster.Sdk.Background.Cancellation;

/// <summary>
/// The one place JobMaster creates cancellation sources, so the tree (runtime → worker → runner → job) and every
/// source deliberately left out of it are visible in this file. Each owner's scope is created from its parent's; the
/// owner takes it itself (the runtime in its constructor, a worker or runner when it starts). Owned by the runtime.
/// </summary>
internal class JobMasterStopScopeFactory
{
    /// <summary>The root of the tree: the runtime's scope.</summary>
    public StopScope ForRoot()
    {
        return StopScope.Create();
    }

    /// <summary>A worker's scope, stopped and aborted with its runtime's.</summary>
    public StopScope ForWorker(IJobMasterBackgroundAgentWorker worker)
    {
        var runtime = worker.Runtime ?? throw new InvalidOperationException($"Worker {worker.AgentWorkerId} has no runtime to link its stop scope to.");
        return StopScope.CreateLinked(runtime.StopScope);
    }

    /// <summary>A runner's scope, stopped and aborted with its worker's.</summary>
    public StopScope ForRunner(IJobMasterBackgroundAgentWorker worker)
    {
        return StopScope.CreateLinked(worker.StopScope);
    }

    /// <summary>A running job's scope, stopped and aborted with its runner's.</summary>
    public StopScope ForJob(IJobMasterRunner runner)
    {
       var scope = StopScope.CreateLinked(runner.StopScope);
       return scope;
    }

    /// <summary>
    /// Cancelled only after <paramref name="limit"/>, on purpose not linked to any scope: for work that must finish
    /// even while stopping (an ack, the cleanup after a stop).
    /// </summary>
    public CancellationTokenSource Timeout(TimeSpan limit)
    {
        return new CancellationTokenSource(limit);
    }
}
