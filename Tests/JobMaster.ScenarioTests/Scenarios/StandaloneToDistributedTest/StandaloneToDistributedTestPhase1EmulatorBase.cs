using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest;

/// <summary>
/// Shared phase-1 logic for every repo-type variant of this scenario (PostgresPure, MySqlPure,
/// SqlServerPure, PostgresNats): schedules jobs against a Standalone all-in-one worker, then
/// immediately stops it -- no waiting, no assertions. Same rationale as DrainModeTest's Phase1:
/// crashing immediately, rather than waiting for jobs to settle into any particular state first,
/// means Phase2 has to recover jobs caught across every stage of the pipeline (PendingSave,
/// OnMaster, InBucket/Processing), not just whichever stage a calibrated wait would have let them
/// reach.
///
/// A concrete scenario only needs to supply its (already kebab-cased) <see cref="ClusterId"/> and
/// <see cref="ContainerName"/>, implement <see cref="StoreState"/> to hand off this run's generated
/// test identifier and scheduled job IDs to its own (namespace-scoped) static state holder for
/// Phase2 to read back, and implement <see cref="BasePhaseEmulator{TPhaseEnum}.Phase"/>.
/// </summary>
public abstract class StandaloneToDistributedTestPhase1EmulatorBase<TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TPhaseEnum : struct, Enum
{
    private const int QtyJobs = 300;

    protected abstract string ClusterId { get; }
    protected abstract string ContainerName { get; }

    /// <summary>Hands this run's test identifier + scheduled job IDs to the concrete variant's own
    /// static state holder, for its Phase2 counterpart to read back -- see that class's own doc
    /// comment for why static state is safe here (single serialized scenario collection).</summary>
    protected abstract void StoreState(string testIdentifier, List<Guid> jobIds);

    public override async Task RunAsync()
    {
        // Generated per run, not a literal constant: every repo-type variant of this scenario
        // shares one Redis instance (ScenarioGlobalEnvironment is a single run-scoped fixture), and
        // xUnit's ScenarioCollection runs them sequentially rather than concurrently -- but Redis
        // data from an earlier variant's run isn't cleared afterward. A literal TestIdentifier
        // would let a later variant's Tracker.WaitForAsync (in Phase2) match an earlier variant's
        // already-recorded executions.
        var testIdentifier = Guid.NewGuid().ToString("N");

        var scheduled = await Runner.ScheduleFor(ContainerName)
            .ScheduleAsync("fast", testIdentifier, QtyJobs, clusterId: ClusterId);

        StoreState(testIdentifier, scheduled.JobIds);

        // The simulated "we're moving off standalone forever": the standalone container is never
        // re-listed in Phase2, but ScenarioRunner never auto-stops a container just because a later
        // phase omits it -- it must be stopped explicitly here.
        await Runner.StopAsync(ContainerName);
    }
}
