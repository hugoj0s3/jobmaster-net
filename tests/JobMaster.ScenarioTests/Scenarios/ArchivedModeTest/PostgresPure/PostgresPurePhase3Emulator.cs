using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using JobMaster.ScenarioTests.Scenarios.ScheduleTest;

namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.PostgresPure;

/// <summary>
/// stray-old restarts as Archived while still holding Phase2's late jobs and schedule (startup must not
/// refuse it: the unfinished-data check only applies when entering Archived), with MigrateTo configured on
/// the Archived cluster. Its Coordinator moves them to stray-new, which runs them -- same job ids, none lost.
/// </summary>
public sealed class PostgresPurePhase3Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<PostgresPurePhases>(global, runner)
{
    private const string ContainerName = "pg-stray";
    private const string StrayOldClusterId = "stray-old";
    private const string StrayNewClusterId = "stray-new";
    private const int SucceededStatus = 5;
    private const int ActiveStatus = 2;

    // The migrate runners tick right after startup; budget a full 5-minute Archived interval on top.
    private static readonly TimeSpan MoveWaitTimeout = TimeSpan.FromMinutes(7);
    private static readonly TimeSpan ExecuteWaitTimeout = TimeSpan.FromMinutes(10);

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase3;

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");
        var lateJobIds = PostgresPureState.LateJobIds;
        lateJobIds.Should().NotBeEmpty("Phase2 must have run first");

        await PollingWaitUtil.WaitUntilAsync(MoveWaitTimeout,
            async () => await api.GetJobCountAsync(StrayOldClusterId, testIdentifier: PostgresPureState.TestIdentifier) == 0,
            "every late job on the archived cluster to be moved to its MigrateTo target",
            pollInterval: TimeSpan.FromSeconds(5));

        await PollingWaitUtil.WaitUntilAsync(MoveWaitTimeout,
            async () => (await api.GetRecurringSchedulesAsync(StrayOldClusterId, PostgresPureState.RecurringTestIdentifier)).Count == 0,
            "the late recurring schedule on the archived cluster to be moved to its MigrateTo target",
            pollInterval: TimeSpan.FromSeconds(5));

        var executions = await Runner.Tracker.WaitForAsync(PostgresPureState.TestIdentifier, lateJobIds.Count, ExecuteWaitTimeout);
        executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        var movedJobs = await api.GetJobsAsync(StrayNewClusterId, testIdentifier: PostgresPureState.TestIdentifier, status: SucceededStatus, countLimit: int.MaxValue);
        movedJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(lateJobIds,
            "the late jobs must keep their ids when moved off the archived cluster");

        var movedSchedules = await api.GetRecurringSchedulesAsync(StrayNewClusterId, PostgresPureState.RecurringTestIdentifier);
        movedSchedules.Should().ContainSingle();
        GuidBase64.Parse(movedSchedules[0].Id).Should().Be(PostgresPureState.LateRecurringScheduleId);
        movedSchedules[0].Status.GetInt32().Should().Be(ActiveStatus);

        await Runner.Tracker.WaitForAsync(PostgresPureState.RecurringTestIdentifier, 1, ExecuteWaitTimeout);

        await Runner.RecurringScheduleFor(ContainerName)
            .CancelRecurringAsync(PostgresPureState.LateRecurringScheduleId, clusterId: StrayNewClusterId);
    }
}
