using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.Postgres;

/// <summary>
/// Schedules jobs against a Standalone all-in-one worker, then immediately stops it -- no
/// waiting, no assertions. Same rationale as DrainModeTest's Phase1: crashing immediately, rather
/// than waiting for jobs to settle into any particular state first, means Phase2 has to recover
/// jobs caught across every stage of the pipeline (PendingSave, OnMaster, InBucket/Processing),
/// not just whichever stage a calibrated wait would have let them reach.
/// </summary>
public sealed class PostgresPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<PostgresPhases>(global, runner)
{
    private const string ClusterId = "standalone-to-dist";
    private const string TestIdentifier = "standalonedist-fast";
    private const int QtyJobs = 300;

    public override PostgresPhases Phase() => PostgresPhases.Phase1;

    public override async Task RunAsync()
    {
        var scheduled = await Runner.ScheduleFor("pg-standalone")
            .ScheduleAsync("fast", TestIdentifier, QtyJobs, clusterId: ClusterId);

        StandaloneToDistributedTestState.ScheduledJobIds = scheduled.JobIds;

        // The simulated "we're moving off standalone forever": pg-standalone is never re-listed
        // in Phase2, but ScenarioRunner never auto-stops a container just because a later phase
        // omits it -- it must be stopped explicitly here.
        await Runner.StopAsync("pg-standalone");
    }
}
