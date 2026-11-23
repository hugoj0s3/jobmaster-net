using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioMatrixTests.Lib;

namespace JobMaster.ScenarioMatrixTests.Scenarios.WorkerLaneTest;

/// <summary>
/// Jobs reach the worker of their lane: one container runs a worker without a lane, another a "billing" worker, on
/// one cluster. Jobs with the lane (on the call, or from the handler's attribute) run only on the billing container,
/// jobs without it only on the other, all of them promptly, not after the no-bucket fallback. Run on a standalone and
/// on a distributed cluster. See scenario.md.
/// </summary>
[Collection(MatrixCollection.Name)]
public sealed class WorkerLaneTests(ScenarioGlobalEnvironment global)
{
    private const int JobsPerKind = 10;

    // Well under the no-bucket fallback (2.5 min + the assign runner's cycle), so a job that only ran through the
    // fallback fails the test: before standalone workers could have a lane, lane jobs took about 3 min 48 s there.
    private static readonly TimeSpan RunTimeout = TimeSpan.FromSeconds(120);

    public static IEnumerable<object[]> Variants() =>
        ScenarioFlavors.AllExcept()
            .Select(flavor => new object[] { flavor.Name, ScenarioTopology.Distributed })
            .Concat(ScenarioFlavors.AllExcept(
                    (ScenarioFlavors.PostgresNats, ScenarioFlavors.StandaloneHasNoAgent),
                    (ScenarioFlavors.HybridNatsRavenDb, ScenarioFlavors.StandaloneHasNoAgent))
                .Select(flavor => new object[] { flavor.Name, ScenarioTopology.Standalone }));

    [Theory]
    [MemberData(nameof(Variants))]
    public async Task Run(string flavor, ScenarioTopology topology)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "WorkerLaneTest");

        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json, selected));

        var phase = topology == ScenarioTopology.Standalone ? "standalone" : "distributed";
        var topo = topology == ScenarioTopology.Standalone ? "std" : "dist";
        await runner.StartPhaseAsync(phase);

        var clusterId = $"mx-{selected.Code}-{topo}-lane";
        var defaultHost = $"mx-{selected.Code}-{topo}-lane-default";
        var billingHost = $"mx-{selected.Code}-{topo}-lane-billing";

        // Identifiers unique to this run: every run in this process shares one Redis.
        var runId = Guid.NewGuid().ToString("N")[..8];
        var noLaneId = $"lane-none-{runId}";
        var callLaneId = $"lane-call-{runId}";
        var attributeLaneId = $"lane-attr-{runId}";

        var noLane = await runner.Schedule.ScheduleAsync("fast", noLaneId, qtyJobs: JobsPerKind, clusterId: clusterId);
        var callLane = await runner.Schedule.ScheduleAsync("fast", callLaneId, qtyJobs: JobsPerKind, clusterId: clusterId, workerLane: "billing");
        var attributeLane = await runner.Schedule.ScheduleAsync("billing-lane", attributeLaneId, qtyJobs: JobsPerKind, clusterId: clusterId);

        await AssertRanOnceOnAsync(runner, noLaneId, noLane.JobIds, defaultHost);
        await AssertRanOnceOnAsync(runner, callLaneId, callLane.JobIds, billingHost);
        await AssertRanOnceOnAsync(runner, attributeLaneId, attributeLane.JobIds, billingHost);
    }

    private static async Task AssertRanOnceOnAsync(ScenarioRunner runner, string testIdentifier, List<Guid> jobIds, string expectedHost)
    {
        jobIds.Should().HaveCount(JobsPerKind);

        var executions = await runner.Tracker.WaitForAsync(testIdentifier, expectedCount: JobsPerKind, timeout: RunTimeout);
        executions.Select(e => e.JobId).Should().BeEquivalentTo(jobIds);

        // Re-read after a settle window: each job ran exactly once, not just at least once.
        await Task.Delay(TimeSpan.FromSeconds(2));
        var all = await runner.Tracker.GetAllAsync(testIdentifier);
        all.Should().HaveCount(JobsPerKind);
        all.Select(e => e.HostId).Should().AllBe(expectedHost, $"jobs '{testIdentifier}' must run on the worker of their lane");
    }
}
