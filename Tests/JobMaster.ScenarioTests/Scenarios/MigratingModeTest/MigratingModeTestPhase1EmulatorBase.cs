using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using JobMaster.ScenarioTests.Scenarios.ScheduleTest;

namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest;

/// <summary>
/// Shared phase-1 logic for every repo-type variant of this scenario (PostgresPure, MySqlPure,
/// SqlServerPure, PostgresNats): one container hosts two clusters (MigrateJobsRunner cross-cluster
/// forwards via <c>JobMasterClusterAwareComponentFactories.TryGetFactory</c>, which only resolves
/// clusters registered in the same process) -- a Migrating source (TargetActiveClusterId set, no
/// agent connections -- just a Coordinator, since a Migrating cluster can never have buckets and
/// every job scheduled there is held OnMaster forever until migrated) and an entirely ordinary
/// Active target with its own Coordinator + Execution worker. Proves the real, unmodified
/// MigrateJobsRunner (30s tick) moves every job across, and that the target's normal pipeline picks
/// them up and finishes them -- no job lost, none duplicated, and the exact same job Id preserved
/// end to end (MigrateJobsRunner's ReassignToCluster only ever changes ClusterId, never re-creates
/// the job). Also covers MigrateRecurringSchedulesRunner the same way: every recurring schedule on
/// a Migrating cluster is immediately Active there (nothing ever plans its occurrences), so it's a
/// migration candidate from the moment it's created.
///
/// A concrete scenario only needs to supply its (already kebab-cased) <see cref="SourceClusterId"/>,
/// <see cref="TargetClusterId"/>, and <see cref="ContainerName"/>, and implement
/// <see cref="BasePhaseEmulator{TPhaseEnum}.Phase"/> -- everything else is identical regardless of
/// which repo type is under test.
/// </summary>
public abstract class MigratingModeTestPhase1EmulatorBase<TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TPhaseEnum : struct, Enum
{
    private const string TimeSpanIntervalExpressionTypeId = "TimeSpanInterval";
    private const string RecurringInterval = "00:00:15";
    private const int QtyJobs = 500;
    private const int SucceededStatus = 5;
    private const int ActiveStatus = 2;

    // MigrateJobsRunner ticks every 30s and moves up to TransferBatchSize (1000) jobs per tick --
    // 500 jobs comfortably fit in one tick. Budgeted generously past a few ticks' worth of heartbeat
    // and scheduling-visibility delay.
    private static readonly TimeSpan MigrateWaitTimeout = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan ExecuteWaitTimeout = TimeSpan.FromMinutes(10);

    protected abstract string SourceClusterId { get; }
    protected abstract string TargetClusterId { get; }
    protected abstract string ContainerName { get; }

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        // Generated per run, not literal constants: every repo-type variant of this scenario shares
        // one Redis instance (ScenarioGlobalEnvironment is a single run-scoped fixture), and xUnit's
        // ScenarioCollection runs them sequentially rather than concurrently -- but Redis data from
        // an earlier variant's run isn't cleared afterward. A literal TestIdentifier would let a
        // later variant's Tracker.WaitForAsync find an earlier variant's already-recorded
        // executions and return immediately, well before this run's own jobs finish.
        var testIdentifier = Guid.NewGuid().ToString("N");
        var recurringTestIdentifier = Guid.NewGuid().ToString("N");

        var scheduled = await Runner.ScheduleFor(ContainerName)
            .ScheduleAsync("fast", testIdentifier, QtyJobs, clusterId: SourceClusterId);
        scheduled.JobIds.Should().HaveCount(QtyJobs);

        var recurring = await Runner.RecurringScheduleFor(ContainerName)
            .CreateRecurringAsync("fast", TimeSpanIntervalExpressionTypeId, RecurringInterval, recurringTestIdentifier, clusterId: SourceClusterId);

        // Migrating clusters never onboard jobs into buckets (AssignJobsToBucketsRunner only runs on
        // Active clusters) -- every job scheduled here starts, and stays, OnMaster until
        // MigrateJobsRunner forwards it to migrating-target. So "source count reaches zero" is
        // exactly the migration-complete signal, no status filter needed.
        await PollingWaitUtil.WaitUntilAsync(MigrateWaitTimeout,
            async () => await api.GetJobCountAsync(SourceClusterId) == 0,
            "every job on the Migrating cluster to be forwarded to the target Active cluster",
            pollInterval: TimeSpan.FromSeconds(5));

        // ScheduleRecurringJobsRunner never plans occurrences on a Migrating cluster either, so this
        // recurring schedule stays Active there, untouched, until MigrateRecurringSchedulesRunner
        // (same 30s tick) forwards it -- "source list is empty" is the migration-complete signal here
        // too.
        await PollingWaitUtil.WaitUntilAsync(MigrateWaitTimeout,
            async () => (await api.GetRecurringSchedulesAsync(SourceClusterId, recurringTestIdentifier)).Count == 0,
            "the recurring schedule on the Migrating cluster to be forwarded to the target Active cluster",
            pollInterval: TimeSpan.FromSeconds(5));

        // Once migrated, it's an entirely ordinary Active cluster from here -- its own Coordinator
        // onboards the jobs into buckets and its own Execution worker runs them to completion.
        var executions = await Runner.Tracker.WaitForAsync(testIdentifier, QtyJobs, ExecuteWaitTimeout);
        executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        var apiJobs = await api.GetJobsAsync(TargetClusterId, testIdentifier: testIdentifier, status: SucceededStatus, countLimit: int.MaxValue);
        apiJobs.Should().HaveCount(QtyJobs);
        apiJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(scheduled.JobIds,
            "migration must preserve the exact same job IDs -- MigrateJobsRunner reassigns ClusterId only, it never re-creates the job with a new Id");

        // Scoped by TestIdentifier -- the recurring schedule (migrated and planned independently,
        // possibly concurrently with this wait) also fires jobs on this same cluster via the same
        // TestAppFastHandler, so an unscoped count would flake once it starts firing.
        (await api.GetJobCountAsync(TargetClusterId, testIdentifier: testIdentifier, status: SucceededStatus)).Should().Be(QtyJobs);
        (await api.GetJobCountAsync(TargetClusterId, testIdentifier: testIdentifier)).Should().Be(QtyJobs);

        // The migrated schedule must be genuinely live on the target, not just a relocated row --
        // its own ScheduleRecurringJobsRunner should have already planned and fired at least one
        // occurrence by now, on the same 15s interval.
        var migratedSchedules = await api.GetRecurringSchedulesAsync(TargetClusterId, recurringTestIdentifier);
        migratedSchedules.Should().ContainSingle();
        var migratedSchedule = migratedSchedules[0];
        GuidBase64.Parse(migratedSchedule.Id).Should().Be(recurring.RecurringScheduleId);
        migratedSchedule.Status.GetInt32().Should().Be(ActiveStatus);

        await Runner.Tracker.WaitForAsync(recurringTestIdentifier, 1, ExecuteWaitTimeout);

        await Runner.RecurringScheduleFor(ContainerName)
            .CancelRecurringAsync(recurring.RecurringScheduleId, clusterId: TargetClusterId);
    }
}
