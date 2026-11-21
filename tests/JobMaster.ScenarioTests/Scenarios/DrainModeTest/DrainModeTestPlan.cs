namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest;

/// <summary>
/// Shared by every phase in <c>DrainModeTest.PostgresDist</c> -- each phase is a separate
/// <c>Activator.CreateInstance</c> instance with no in-memory hand-off from the one before it
/// (<c>BaseScenarioEmulator.CreatePhaseEmulators</c>), so every phase recomputes this same
/// deterministic plan instead of relying on state Phase1 alone would otherwise hold (e.g. the
/// job IDs returned by scheduling). Job-set correctness across phases is verified against the API
/// instead, filtered by <c>ClusterId + TestIdentifier</c>, which persists in the database across
/// every container restart.
///
/// 5000 jobs total, deliberately above <c>JobMasterDefaults.Worker.TransferBatchSize</c> (1000 --
/// the number of jobs pulled from master per DB round-trip), so onboarding the whole batch requires
/// multiple transfer cycles, not just one. <c>JM_DRAIN_JOBS_MULTIPLIER</c> (default 1) scales every batch, for a
/// heavier run (e.g. 5 → 25k jobs, more of them in flight when the executors are stopped).
/// </summary>
public static class DrainModeTestPlan
{
    public static readonly int JobsMultiplier =
        int.TryParse(Environment.GetEnvironmentVariable("JM_DRAIN_JOBS_MULTIPLIER"), out var m) && m > 0 ? m : 1;

    // Jobs per schedule request: the app schedules a request's jobs concurrently, and the HttpClient times out at 100 s.
    public const int MaxJobsPerRequest = 3500;

    public static readonly IReadOnlyList<DrainModeTestBatch> Batches = new List<DrainModeTestBatch>
    {
        new("fast", 3500 * JobsMultiplier, Priority: 3, TestIdentifier: "drainload-fast"),     // Medium
        new("normal", 1250 * JobsMultiplier, Priority: 4, TestIdentifier: "drainload-normal"), // High
        new("slow", 250 * JobsMultiplier, Priority: 2, TestIdentifier: "drainload-slow"),      // Low
    };

    public static readonly int TotalJobs = Batches.Sum(b => b.QtyJobs);

    public static readonly IReadOnlyDictionary<string, string> HandlerTypeToJobDefinitionId = new Dictionary<string, string>
    {
        ["fast"] = "TestApp.Fast",
        ["normal"] = "TestApp.Normal",
        ["slow"] = "TestApp.Slow"
    };
}

public sealed record DrainModeTestBatch(string HandlerType, int QtyJobs, int Priority, string TestIdentifier);
