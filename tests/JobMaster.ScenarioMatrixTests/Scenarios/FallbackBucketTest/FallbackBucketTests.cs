using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioMatrixTests.Lib;

namespace JobMaster.ScenarioMatrixTests.Scenarios.FallbackBucketTest;

/// <summary>
/// A coordinator-only cluster -- no agent connection declared at all -- still runs its jobs: once no real bucket can
/// be found for a job for over <c>JobMasterConstants.NoBucketFallbackThreshold</c> (2.5 min), the coordinator creates a
/// fallback bucket on a reserved, master-backed connection and runs it itself. See scenario.md.
/// </summary>
[Collection(MatrixCollection.Name)]
public sealed class FallbackBucketTests(ScenarioGlobalEnvironment global)
{
    private const int QtyJobs = 30;
    private const int SucceededStatus = 5;
    private const int FallbackBucketType = 2;

    // The threshold is a fixed 2.5 min framework constant; budget well past it plus onboarding and execution.
    private static readonly TimeSpan ExecuteWaitTimeout = TimeSpan.FromMinutes(20);

    public static IEnumerable<object[]> Flavors() =>
        ScenarioFlavors.AllExcept(
                (ScenarioFlavors.PostgresNats, ScenarioFlavors.NoAgentConnection),
                (ScenarioFlavors.HybridNatsRavenDb, ScenarioFlavors.NoAgentConnection))
            .Select(flavor => new object[] { flavor.Name });

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Run(string flavor)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "FallbackBucketTest");

        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json, selected));
        await runner.StartPhaseAsync("phase-1");

        var clusterId = $"mx-{selected.Code}-fallback";
        var api = runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");
        var testIdentifier = Guid.NewGuid().ToString("N");

        var scheduled = await runner.ScheduleFor(clusterId).ScheduleAsync("fast", testIdentifier, QtyJobs, clusterId: clusterId);
        scheduled.JobIds.Should().HaveCount(QtyJobs);

        var executions = await runner.Tracker.WaitForAsync(testIdentifier, QtyJobs, ExecuteWaitTimeout);
        executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        var apiJobs = await api.GetJobsAsync(clusterId, testIdentifier: testIdentifier, status: SucceededStatus, countLimit: int.MaxValue);
        apiJobs.Should().HaveCount(QtyJobs);
        apiJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(scheduled.JobIds,
            "the fallback bucket dispatches jobs directly, it never re-creates them with a new Id");

        var buckets = await api.GetBucketsAsync(clusterId, countLimit: int.MaxValue);
        buckets.Should().ContainSingle(b => b.BucketType.GetInt32() == FallbackBucketType,
            "no real bucket can exist on a coordinator-only cluster -- every job must have gone through the one fallback bucket");
    }
}
