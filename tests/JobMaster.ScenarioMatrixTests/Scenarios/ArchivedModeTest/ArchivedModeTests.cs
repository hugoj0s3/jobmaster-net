using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using JobMaster.ScenarioTests.Scenarios.ScheduleTest;

namespace JobMaster.ScenarioMatrixTests.Scenarios.ArchivedModeTest;

/// <summary>
/// The archived-mode safety net: late work left on a cluster that became Archived (the scheduler's mode check isn't
/// atomic with the write) is moved to its <c>TargetActiveClusterId</c> and runs there, same ids, none lost. See
/// scenario.md.
/// </summary>
[Collection(MatrixCollection.Name)]
public sealed class ArchivedModeTests(ScenarioGlobalEnvironment global)
{
    private const int QtyJobs = 20;
    private const int ArchivedMode = 3;
    private const int SucceededStatus = 5;
    private const int ActiveStatus = 2;

    // The migrate runners tick right after startup; budget a few 2.5 min Archived intervals on top.
    private static readonly TimeSpan MoveWaitTimeout = TimeSpan.FromMinutes(7);
    private static readonly TimeSpan ExecuteWaitTimeout = TimeSpan.FromMinutes(10);

    public static IEnumerable<object[]> Flavors() =>
        ScenarioFlavors.AllExcept().Select(flavor => new object[] { flavor.Name });

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Run(string flavor)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "ArchivedModeTest");

        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json, selected));

        var strayOldClusterId = $"mx-{selected.Code}-stray-old";
        var strayNewClusterId = $"mx-{selected.Code}-stray-new";
        var containerName = $"mx-{selected.Code}-stray";
        var api = runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");
        var testIdentifier = Guid.NewGuid().ToString("N");
        var recurringTestIdentifier = Guid.NewGuid().ToString("N");

        // Phase 1: stray-old is Migrating (to stray-new) with no coordinator, so the jobs and the schedule written
        // here stay on it. Then its stored mode is switched to Archived in the database directly: the switch landed
        // right after those writes passed the mode check.
        await runner.StartPhaseAsync("phase-1");

        var scheduled = await runner.ScheduleFor(containerName)
            .ScheduleAsync("fast", testIdentifier, QtyJobs, clusterId: strayOldClusterId);
        scheduled.JobIds.Should().HaveCount(QtyJobs);

        var recurring = await runner.RecurringScheduleFor(containerName)
            .CreateRecurringAsync("fast", "TimeSpanInterval", "00:00:15", recurringTestIdentifier, clusterId: strayOldClusterId);

        (await api.GetJobCountAsync(strayOldClusterId, testIdentifier: testIdentifier)).Should().Be(QtyJobs);
        (await api.GetRecurringSchedulesAsync(strayOldClusterId, recurringTestIdentifier)).Should().ContainSingle();

        await runner.StopAsync(containerName);

        (await selected.SetStoredClusterModeAsync(global, "StrayOld", strayOldClusterId, ArchivedMode)).Should().Be(1,
            "stray-old's stored ClusterMode must be switched to Archived");

        // Phase 2: stray-old restarts Archived (target stray-new) while still holding the late work; startup must not
        // refuse it (with a target, the unfinished-data check is skipped). Its coordinator moves the work to
        // stray-new, which runs it.
        await runner.StartPhaseAsync("phase-2");

        await PollingWaitUtil.WaitUntilAsync(MoveWaitTimeout,
            async () => await api.GetJobCountAsync(strayOldClusterId, testIdentifier: testIdentifier) == 0,
            "every late job on the archived cluster to be moved to its MigrateTo target",
            pollInterval: TimeSpan.FromSeconds(5));

        await PollingWaitUtil.WaitUntilAsync(MoveWaitTimeout,
            async () => (await api.GetRecurringSchedulesAsync(strayOldClusterId, recurringTestIdentifier)).Count == 0,
            "the late recurring schedule on the archived cluster to be moved to its MigrateTo target",
            pollInterval: TimeSpan.FromSeconds(5));

        var executions = await runner.Tracker.WaitForAsync(testIdentifier, QtyJobs, ExecuteWaitTimeout);
        executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        var movedJobs = await api.GetJobsAsync(strayNewClusterId, testIdentifier: testIdentifier, status: SucceededStatus, countLimit: int.MaxValue);
        movedJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(scheduled.JobIds,
            "the late jobs must keep their ids when moved off the archived cluster");

        var movedSchedules = await api.GetRecurringSchedulesAsync(strayNewClusterId, recurringTestIdentifier);
        movedSchedules.Should().ContainSingle();
        GuidBase64.Parse(movedSchedules[0].Id).Should().Be(recurring.RecurringScheduleId);
        movedSchedules[0].Status.GetInt32().Should().Be(ActiveStatus);

        await runner.Tracker.WaitForAsync(recurringTestIdentifier, 1, ExecuteWaitTimeout);

        await runner.RecurringScheduleFor(containerName)
            .CancelRecurringAsync(recurring.RecurringScheduleId, clusterId: strayNewClusterId);
    }
}
