namespace JobMaster.Benchmarks.Common.Recording;

/// <summary>
/// Shared "real job" simulation used by every framework's no-op handler (JobMasterHost,
/// HangfireHost, QuartzHost) so the benchmark measures scheduling/dispatch overhead against a
/// job body with realistic duration, not an instant no-op -- a job that does nothing makes any
/// fixed per-job coordination cost (locking, throttling, retries) look disproportionately large
/// compared to what a real workload would show.
/// </summary>
public static class SimulatedJobWork
{
    /// <summary>Delays a random 250-500ms (5-10, step 50).</summary>
    public static Task DelayAsync() => Task.Delay(Random.Shared.Next(5, 11) * 50);
}
