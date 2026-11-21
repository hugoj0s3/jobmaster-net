using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using JobMaster.ScenarioTests.Scenarios.ScheduleTest;

namespace JobMaster.ScenarioMatrixTests.Scenarios.MigratingModeTest;

/// <summary>
/// Every job and recurring schedule on a Migrating cluster is moved to its target Active cluster and finished there,
/// with the same ids, none lost and none duplicated. One container hosts both clusters (MigrateJobsRunner forwards
/// through <c>JobMasterClusterAwareComponentFactories.TryGetFactory</c>, which only resolves clusters registered in
/// the same process). See scenario.md.
/// </summary>
[Collection(MatrixCollection.Name)]
public sealed class MigratingModeTests(ScenarioGlobalEnvironment global)
{
    private const string TimeSpanIntervalExpressionTypeId = "TimeSpanInterval";
    private const string RecurringInterval = "00:00:15";
    private const int QtyJobs = 500;
    private const int SucceededStatus = 5;
    private const int ActiveStatus = 2;

    // MigrateJobsRunner ticks every 30 s and moves up to TransferBatchSize (1000) jobs per tick: 500 fit in one tick.
    private static readonly TimeSpan MigrateWaitTimeout = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan ExecuteWaitTimeout = TimeSpan.FromMinutes(10);

    public static IEnumerable<object[]> Flavors() =>
        ScenarioFlavors.AllExcept().Select(flavor => new object[] { flavor.Name });

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Run(string flavor)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "MigratingModeTest");

        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json, selected));
        await runner.StartPhaseAsync("phase-1");

        var sourceClusterId = $"mx-{selected.Code}-mig-source";
        var targetClusterId = $"mx-{selected.Code}-mig-target";
        var containerName = $"mx-{selected.Code}-migration";
        var api = runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        var testIdentifier = Guid.NewGuid().ToString("N");
        var recurringTestIdentifier = Guid.NewGuid().ToString("N");

        var scheduled = await runner.ScheduleFor(containerName)
            .ScheduleAsync("fast", testIdentifier, QtyJobs, clusterId: sourceClusterId);
        scheduled.JobIds.Should().HaveCount(QtyJobs);

        var recurring = await runner.RecurringScheduleFor(containerName)
            .CreateRecurringAsync("fast", TimeSpanIntervalExpressionTypeId, RecurringInterval, recurringTestIdentifier, clusterId: sourceClusterId);

        // A Migrating cluster never onboards jobs (no buckets), so every job stays on master there until it's
        // forwarded: "source count reaches zero" is exactly the migration-complete signal.
        await PollingWaitUtil.WaitUntilAsync(MigrateWaitTimeout,
            async () => await api.GetJobCountAsync(sourceClusterId) == 0,
            "every job on the Migrating cluster to be forwarded to the target Active cluster",
            pollInterval: TimeSpan.FromSeconds(5));

        // Nothing plans occurrences on a Migrating cluster either, so the schedule stays there until forwarded.
        await PollingWaitUtil.WaitUntilAsync(MigrateWaitTimeout,
            async () => (await api.GetRecurringSchedulesAsync(sourceClusterId, recurringTestIdentifier)).Count == 0,
            "the recurring schedule on the Migrating cluster to be forwarded to the target Active cluster",
            pollInterval: TimeSpan.FromSeconds(5));

        // Once migrated, the target's own coordinator onboards the jobs and its execution worker runs them.
        var executions = await runner.Tracker.WaitForAsync(testIdentifier, QtyJobs, ExecuteWaitTimeout);
        executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        var apiJobs = await api.GetJobsAsync(targetClusterId, testIdentifier: testIdentifier, status: SucceededStatus, countLimit: int.MaxValue);
        apiJobs.Should().HaveCount(QtyJobs);
        apiJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(scheduled.JobIds,
            "migration must preserve the exact same job IDs -- MigrateJobsRunner reassigns ClusterId only, it never re-creates the job with a new Id");

        // Scoped by test identifier: the migrated schedule fires jobs on the same cluster with the same handler.
        (await api.GetJobCountAsync(targetClusterId, testIdentifier: testIdentifier, status: SucceededStatus)).Should().Be(QtyJobs);
        (await api.GetJobCountAsync(targetClusterId, testIdentifier: testIdentifier)).Should().Be(QtyJobs);

        // The migrated schedule is live on the target, not just a relocated row: same id, Active, and it fires there.
        var migratedSchedules = await api.GetRecurringSchedulesAsync(targetClusterId, recurringTestIdentifier);
        migratedSchedules.Should().ContainSingle();
        GuidBase64.Parse(migratedSchedules[0].Id).Should().Be(recurring.RecurringScheduleId);
        migratedSchedules[0].Status.GetInt32().Should().Be(ActiveStatus);

        await runner.Tracker.WaitForAsync(recurringTestIdentifier, 1, ExecuteWaitTimeout);

        await runner.RecurringScheduleFor(containerName)
            .CancelRecurringAsync(recurring.RecurringScheduleId, clusterId: targetClusterId);
    }
}
