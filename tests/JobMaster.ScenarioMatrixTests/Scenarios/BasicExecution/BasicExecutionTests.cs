using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioMatrixTests.Lib;

namespace JobMaster.ScenarioMatrixTests.Scenarios.BasicExecution;

[Collection(MatrixCollection.Name)]
public sealed class BasicExecutionTests(ScenarioGlobalEnvironment global)
{
    public static IEnumerable<object[]> Flavors() =>
        ScenarioFlavors.AllExcept(
                (ScenarioFlavors.PostgresNats, ScenarioFlavors.StandaloneHasNoAgent),
                (ScenarioFlavors.HybridNatsRavenDb, ScenarioFlavors.StandaloneHasNoAgent))
            .Select(flavor => new object[] { flavor.Name });

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Run(string flavor)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "BasicExecution");

        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json, selected));
        await runner.StartPhaseAsync("phase-1");

        var clusterId = $"mx-{selected.Code}-basic-exec";
        var testIdentifier = Guid.NewGuid().ToString("N");

        var scheduled = await runner.Schedule.ScheduleAsync("fast", testIdentifier, qtyJobs: 1, clusterId: clusterId);
        scheduled.JobIds.Should().HaveCount(1);

        var executions = await runner.Tracker.WaitForAsync(testIdentifier, expectedCount: 1, timeout: TimeSpan.FromSeconds(90));
        executions.Select(e => e.JobId).Should().Equal(scheduled.JobIds);

        // Re-read after a settle window: the job ran exactly once, not just at least once.
        await Task.Delay(TimeSpan.FromSeconds(2));
        var all = await runner.Tracker.GetAllAsync(testIdentifier);
        all.Should().HaveCount(1);

        runner.Api.Should().NotBeNull();
        var apiExecutions = await runner.Api!.GetJobExecutionsAsync(clusterId, scheduled.JobIds[0]);
        apiExecutions.Should().NotBeEmpty();
        
        apiExecutions.Select(x => GuidBase64.Parse(x.JobId)).ToList().Should().BeEquivalentTo(scheduled.JobIds);
    }
}
