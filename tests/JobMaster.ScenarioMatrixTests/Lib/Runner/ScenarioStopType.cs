namespace JobMaster.ScenarioMatrixTests.Lib;

/// <summary>How a scenario stops an app container.</summary>
public enum ScenarioStopType
{
    /// <summary>
    /// <c>docker stop</c>: SIGTERM, then SIGKILL after the grace period. The app's own shutdown runs (workers stop,
    /// buckets handed over, buffered jobs flushed back to master), like a deploy or a restart.
    /// </summary>
    SigTerm,

    /// <summary>
    /// SIGKILL, sent by Docker: the process ends at once and nothing of its shutdown runs, like a crash or a lost
    /// machine. Recovery comes only from the other workers (lease expiry, dead-worker detection, deadlines).
    /// </summary>
    Crash
}
