using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest;

/// <summary>
/// Shared phase-2 logic for every "Pure" schedule test: exercises DataRetentionTtl-based purge
/// (<see cref="TtlOnlyClusterId"/>), a real drain-to-completion lifecycle
/// (<see cref="DrainClusterId"/>), and an unaffected control (<see cref="ControlClusterId"/>), all
/// against the jobs Phase1 already scheduled and finished. By the time <see cref="RunAsync"/> runs,
/// the base framework has already called <c>ScenarioRunner.StartPhaseAsync</c> for this phase,
/// which (via the auto-stop-by-name behavior in <c>ScenarioRunner.StartContainerAsync</c>) has
/// already stopped every Phase1 container this phase re-lists and replaced them with reconfigured
/// ones. Three clusters, three different roles:
///
///  - <see cref="TtlOnlyClusterId"/>: gets DataRetentionTtl set, keeps its existing (non-Coordinator)
///    worker. Standalone clusters can't run Coordinator mode at all
///    (IClusterStandaloneConfigSelector.AddWorker has no WorkerMode parameter), but the TTL-purge
///    runner (DeleteOldFinalJobsRunner) is started from LoadCoordinatorRunnersAsync regardless of
///    Full/Coordinator mode, so a plain TTL check still works here.
///  - <see cref="DrainClusterId"/>: no DataRetentionTtl at all -- its purpose here is purely the
///    connection-retirement lifecycle, decoupled from TTL. Its container now runs FOUR workers in
///    one process: a Coordinator worker (no AgentConnectionName) plus its original 3 workers
///    switched to Drain mode (still with their AgentConnectionName). This mirrors the only way
///    JobMaster actually supports retiring a connection: AssignedLostBucketsRunner (which reassigns
///    a dead worker's orphaned buckets) only runs under Coordinator/Full mode, never under a lone
///    Drain worker, so a Coordinator has to be present to hand the old Full worker's now-Lost
///    buckets to the new Drain workers. This phase waits for that real drain (Lost -> ReadyToDrain
///    -> Draining -> ReadyToDelete -> destroyed, all unmodified JobMaster machinery) to actually
///    finish -- i.e. this cluster's bucket count reaches zero -- before Phase3 retires the Drain
///    workers themselves.
///  - <see cref="ControlClusterId"/>: restarted with the *same* config Phase1 used -- no
///    DataRetentionTtl, no worker-mode change. Restarting it (rather than leaving Phase1's container
///    running untouched) is deliberate: it proves DataRetentionTtl itself causes the purge, not
///    merely the app container restarting.
///
/// <see cref="TtlOnlyClusterId"/> is optional (null by default) -- a scenario whose master storage
/// is already covered by an existing *Pure scenario (TTL purge is pure master-side logic) can skip
/// it entirely and test only the drain/control pair, e.g. to cover a different agent/transport
/// provider's drain-runner implementations without redundantly retesting TTL purge.
/// </summary>
public abstract class DataRetentionPhase2EmulatorBase<TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TPhaseEnum : struct, Enum
{
    // JobMasterDefaults.MinDataRetentionTtl is 10 minutes -- the JSON config for TtlOnlyClusterId
    // must set exactly this (the framework's floor, so the fastest allowed). The purge runner's own
    // poll interval is derived from the TTL itself (max(5min, min(ttl/2, 1hr)) => 5 minutes at a
    // 10-minute TTL), so budget the wait generously past both the TTL and at least one extra tick.
    private static readonly TimeSpan PurgeWaitTimeout = TimeSpan.FromMinutes(20);

    // A real drain has to clear: worker-death detection, Lost->ReadyToDrain reassignment, Draining,
    // the existing 10-minute BucketNoJobsBeforeReadyToDelete wait, then the destroy runner's own
    // tick -- all real, unmodified JobMaster timing, not a shortcut.
    private static readonly TimeSpan DrainWaitTimeout = TimeSpan.FromMinutes(30);

    // Null when this scenario has no TTL-purge cluster to test -- e.g. a non-SQL-master transport
    // scenario where TTL purge (DeleteOldFinalJobsRunner) is pure master-side logic already proven
    // by the SQL *Pure scenarios; retesting it there wouldn't exercise anything transport-specific.
    protected virtual string? TtlOnlyClusterId => null;
    protected abstract string DrainClusterId { get; }
    protected abstract string ControlClusterId { get; }

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        // Sanity: Phase1 must have actually left completed jobs behind on every cluster this phase
        // touches, and the reconfigured containers must still see that same pre-existing data (same
        // database, untouched by the app-container restart) before any purge/drain has had a chance
        // to run.
        if (TtlOnlyClusterId is not null)
        {
            var ttlOnlyBefore = await api.GetJobCountAsync(TtlOnlyClusterId);
            ttlOnlyBefore.Should().BeGreaterThan(0, "Phase1 should have left completed jobs on this cluster");
        }

        var drainBefore = await api.GetJobCountAsync(DrainClusterId);
        var controlBefore = await api.GetJobCountAsync(ControlClusterId);
        drainBefore.Should().BeGreaterThan(0, "Phase1 should have left completed jobs on this cluster");
        controlBefore.Should().BeGreaterThan(0, "Phase1 should have left completed jobs on this cluster");

        // The Drain workers are freshly started and should be actively heartbeating -- proves the
        // 4-worker (Coordinator + 3 Drain) reconfiguration actually took effect before we wait on
        // it. Excludes the Coordinator's own auto-created reserved fallback connection, which is
        // never heartbeated by design.
        var drainConnections = (await api.GetAgentConnectionsAsync(DrainClusterId)).ExcludingReserved().ToList();
        drainConnections.Should().NotBeEmpty();
        drainConnections.Should().OnlyContain(c => c.IsAlive);

        if (TtlOnlyClusterId is not null)
        {
            await PollingWaitUtil.WaitUntilAsync(PurgeWaitTimeout,
                async () => await api.GetJobCountAsync(TtlOnlyClusterId) == 0,
                $"jobs on {TtlOnlyClusterId} to be purged after DataRetentionTtl");
        }

        // The control cluster was restarted with the same config Phase1 used -- no TTL, no mode
        // change -- so its Phase1 data must be completely unaffected. Proves DataRetentionTtl
        // itself causes the purge, not merely a container restart.
        (await api.GetJobCountAsync(ControlClusterId)).Should().Be(controlBefore);

        // DrainClusterId has no TTL -- its jobs are untouched throughout. What's under test here is
        // the real drain lifecycle destroying every bucket, an entirely separate mechanism from job
        // retention.
        (await api.GetJobCountAsync(DrainClusterId)).Should().Be(drainBefore);

        await PollingWaitUtil.WaitUntilAsync(DrainWaitTimeout,
            async () => await api.GetBucketCountAsync(DrainClusterId) == 0,
            $"every bucket on {DrainClusterId} to be destroyed by the real drain lifecycle");
    }
}
