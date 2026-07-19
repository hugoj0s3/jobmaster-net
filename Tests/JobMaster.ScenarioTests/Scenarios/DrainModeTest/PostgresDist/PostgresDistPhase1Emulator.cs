using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.PostgresDist;

/// <summary>
/// Schedules all 5000 jobs, then immediately stops all 3 executors -- no waiting, no assertions.
/// This is deliberate: waiting for jobs to settle into any particular state (flushed off
/// PendingSave, onboarded off OnMaster, etc.) before crashing would mean the crash only ever hits
/// jobs already onboarded into a bucket, exercising just PollingDrainProcessingJobsRunner. Crashing
/// immediately means jobs are caught across every stage of the pipeline -- some still PendingSave
/// (agent-side transport only, not yet in the master store), some OnMaster, some already
/// InBucket/Onboarded/Queued/Processing -- so Phase2 has to recover through both
/// PollingDrainSavePendingJobsRunner and PollingDrainProcessingJobsRunner, not just one of them.
/// </summary>
public sealed class PostgresDistPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<PostgresDistPhases>(global, runner)
{
    private const string ClusterId = "postgres-drain-load";
    private static readonly string[] ExecutorContainers = ["pg-executor-1", "pg-executor-2", "pg-executor-3"];

    public override PostgresDistPhases Phase() => PostgresDistPhases.Phase1;

    public override async Task RunAsync()
    {
        foreach (var batch in DrainModeTestPlan.Batches)
        {
            await Runner.ScheduleFor("pg-executor-1")
                .ScheduleAsync(batch.HandlerType, batch.TestIdentifier, qtyJobs: batch.QtyJobs, clusterId: ClusterId, priority: batch.Priority);
        }

        // The simulated crash: none of the 3 executor containers are re-listed in Phase2, but
        // ScenarioRunner never auto-stops a container just because a later phase omits it -- each
        // must be stopped explicitly here.
        foreach (var containerName in ExecutorContainers)
        {
            await Runner.StopAsync(containerName);
        }
    }
}
