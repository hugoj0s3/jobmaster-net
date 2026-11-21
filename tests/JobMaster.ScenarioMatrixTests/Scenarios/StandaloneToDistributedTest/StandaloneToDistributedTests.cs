using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioMatrixTests.Scenarios.StandaloneToDistributedTest;

/// <summary>
/// Moving a cluster off standalone loses nothing: a standalone worker stops right after scheduling, with jobs caught
/// at every stage, and a distributed topology (coordinator + executor on a new agent connection) takes over the same
/// cluster and database and runs every job exactly once. Run with both stop types. See scenario.md.
/// </summary>
[Collection(MatrixCollection.Name)]
public sealed class StandaloneToDistributedTests(ScenarioGlobalEnvironment global)
{
    private const int QtyJobs = 300;
    private const int SucceededStatus = 5;
    private const string AgentConnectionName = "agent-1";

    // Past the ~10 min ProcessDeadline backstop plus onboarding and execution, though the auto-drain path should
    // win in practice.
    private static readonly TimeSpan FinalizeTimeout = TimeSpan.FromMinutes(20);

    public static IEnumerable<object[]> Variants() =>
        ScenarioFlavors.AllExcept()
            .SelectMany(flavor => ScenarioStopTypes.All.Select(stopType => new object[] { flavor.Name, stopType }));

    [Theory]
    [MemberData(nameof(Variants))]
    public async Task Run(string flavor, ScenarioStopType stopType)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "StandaloneToDistributedTest");

        // Per stop type: its own cluster and master database, so the two runs on one flavor never count each other's jobs.
        var stop = stopType == ScenarioStopType.Crash ? "crash" : "sig";
        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json.Replace("{{stop}}", stop), selected));

        var clusterId = $"mx-{selected.Code}-{stop}-s2d";
        var standalone = $"{clusterId}-standalone";
        var api = runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");
        var testIdentifier = Guid.NewGuid().ToString("N");

        // Phase 1: schedule against the standalone worker, then stop it at once, without waiting for any job to
        // settle, so phase 2 recovers jobs from every stage (pending save, on master, in a bucket, running).
        await runner.StartPhaseAsync("phase-1");
        var scheduled = await runner.ScheduleFor(standalone).ScheduleAsync("fast", testIdentifier, QtyJobs, clusterId: clusterId);
        scheduled.JobIds.Should().HaveCount(QtyJobs);
        await runner.StopAsync(standalone, stopType);

        // Phase 2: a distributed topology takes over the same cluster and database. At startup JobMaster finds buckets
        // still owned by the standalone worker and adds a drain worker on the reserved standalone connection; those
        // buckets go Lost, then that drainer takes them. Jobs still in a bucket the drain missed come back when their
        // ProcessDeadline passes. Either way they're assigned to the new bucket on agent-1.
        await runner.StartPhaseAsync("phase-2");

        var executions = await runner.Tracker.WaitForAsync(testIdentifier, QtyJobs, FinalizeTimeout);
        executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        var apiJobs = await api.GetJobsAsync(clusterId, testIdentifier: testIdentifier, status: SucceededStatus, countLimit: int.MaxValue);
        apiJobs.Should().HaveCount(QtyJobs);
        apiJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(scheduled.JobIds,
            "recovery must preserve the exact same job IDs -- no job lost, none executed twice, across the standalone-to-distributed transition");

        (await api.GetJobCountAsync(clusterId, testIdentifier: testIdentifier, status: SucceededStatus)).Should().Be(QtyJobs);
        (await api.GetJobCountAsync(clusterId, testIdentifier: testIdentifier)).Should().Be(QtyJobs);

        var connections = (await api.GetAgentConnectionsAsync(clusterId)).ExcludingReserved().ToList();
        connections.Should().ContainSingle(c => c.Name == AgentConnectionName && c.IsAlive);
    }
}
