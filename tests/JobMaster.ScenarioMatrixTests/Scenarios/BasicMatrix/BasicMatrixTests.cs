using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioMatrixTests.Scenarios.BasicMatrix;

[Collection(MatrixCollection.Name)]
public sealed class BasicMatrixTests(ScenarioGlobalEnvironment global)
{
    private const int JobCount = 30;

    public static IEnumerable<object[]> Flavors() =>
        ScenarioFlavors.AllExcept().Select(flavor => new object[] { flavor.Name });

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Run(string flavor)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "BasicMatrix");

        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json, selected));
        await runner.StartPhaseAsync("phase-1");

        var clusterId = $"mx-{selected.Code}-basic";
        var testIdentifier = Guid.NewGuid().ToString("N");

        var scheduled = await runner.Schedule.ScheduleAsync("fast", testIdentifier, qtyJobs: JobCount, clusterId: clusterId);
        scheduled.JobIds.Should().HaveCount(JobCount);

        // A fresh distributed cluster first registers its workers and buckets, so the first pickup takes longer
        // than on a standalone one.
        var executions = await runner.Tracker.WaitForAsync(testIdentifier, expectedCount: JobCount, timeout: TimeSpan.FromMinutes(5));
        executions.Select(e => e.JobId).Should().BeEquivalentTo(scheduled.JobIds);

        // Re-read after a settle window: each job ran exactly once, not just at least once.
        await Task.Delay(TimeSpan.FromSeconds(5));
        var all = await runner.Tracker.GetAllAsync(testIdentifier);
        all.Should().HaveCount(JobCount);
        all.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        runner.Api.Should().NotBeNull();
        var apiExecutions = await runner.Api!.GetJobExecutionsAsync(clusterId, scheduled.JobIds[0]);
        apiExecutions.Should().NotBeEmpty();
    }
}
