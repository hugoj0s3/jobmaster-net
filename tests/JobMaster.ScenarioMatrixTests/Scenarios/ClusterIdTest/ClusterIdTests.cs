using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioMatrixTests.Scenarios.ClusterIdTest;

/// <summary>
/// A job or static schedule lands on the right cluster when its cluster id comes from the handler, definition or
/// profile instead of the call. See scenario.md.
/// </summary>
[Collection(MatrixCollection.Name)]
public sealed class ClusterIdTests(ScenarioGlobalEnvironment global)
{
    private const int QtyJobs = 5;

    // Must match TargetTestClusterIdApp's literals (different process/project); the app's ClusterIds.Target is a
    // compile-time constant, so the target cluster keeps its name on every flavor (own databases per flavor).
    private const string TargetClusterId = "cluster-id-target";
    private const string ProfileTestIdentifier = "cluster-id-profile";
    private const string AttributeRecurringTestIdentifier = "cluster-id-attribute-recurring";
    private const string ProfileJobDefinitionId = "ClusterIdApp.ProfileTick";
    private const string AttributeRecurringJobDefinitionId = "ClusterIdApp.AttributeRecurring";

    private static readonly TimeSpan ExecuteWaitTimeout = TimeSpan.FromMinutes(3);

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
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "ClusterIdTest");

        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json, selected));
        await runner.StartPhaseAsync("phase-1");

        var defaultClusterId = $"mx-{selected.Code}-cid-default";
        var api = runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");
        var schedule = runner.ScheduleFor($"mx-{selected.Code}-cluster-id");

        // The static schedules' identifiers are fixed literals in the app; clear what an earlier flavor recorded.
        // They keep firing every 15 s, so clearing after the container started loses nothing that matters.
        await runner.Tracker.ClearAsync(ProfileTestIdentifier);
        await runner.Tracker.ClearAsync(AttributeRecurringTestIdentifier);

        // Nothing below passes a clusterId.
        var attributeId = Guid.NewGuid().ToString("N");
        var advancedId = Guid.NewGuid().ToString("N");
        var defaultId = Guid.NewGuid().ToString("N");

        var attribute = await schedule.ScheduleAsync("attribute", attributeId, QtyJobs);
        var advanced = await schedule.ScheduleAsync("advanced", advancedId, QtyJobs);
        var control = await schedule.ScheduleAsync("default", defaultId, QtyJobs);

        await AssertRanOnAsync(runner, attributeId, QtyJobs, TargetClusterId, "one-off via [JobMasterClusterId]");
        await AssertRanOnAsync(runner, advancedId, QtyJobs, TargetClusterId, "one-off via Advanced definition Config.ClusterId");
        await AssertRanOnAsync(runner, defaultId, QtyJobs, defaultClusterId, "one-off with no cluster id (control)");

        await AssertJobsStoredOnAsync(api, attributeId, attribute.JobIds, TargetClusterId, defaultClusterId);
        await AssertJobsStoredOnAsync(api, advancedId, advanced.JobIds, TargetClusterId, defaultClusterId);
        await AssertJobsStoredOnAsync(api, defaultId, control.JobIds, defaultClusterId, TargetClusterId);

        // Static schedules are registered at startup, so they already exist by now.
        var targetSchedules = await api.GetRecurringSchedulesAsync(TargetClusterId);
        var defaultSchedules = await api.GetRecurringSchedulesAsync(defaultClusterId);

        targetSchedules.Should().ContainSingle(s => s.JobDefinitionId == ProfileJobDefinitionId,
            "the profile's ClusterId should register its schedule on the target cluster");
        targetSchedules.Should().ContainSingle(s => s.JobDefinitionId == AttributeRecurringJobDefinitionId,
            "[JobMasterClusterId] on a [TimeSpanIntervalSchedule] handler should register its schedule on the target cluster");
        defaultSchedules.Should().NotContain(s => s.JobDefinitionId == ProfileJobDefinitionId || s.JobDefinitionId == AttributeRecurringJobDefinitionId,
            "neither static schedule should be registered on the default cluster");

        await AssertRanOnAsync(runner, ProfileTestIdentifier, 1, TargetClusterId, "static recurring via profile ClusterId");
        await AssertRanOnAsync(runner, AttributeRecurringTestIdentifier, 1, TargetClusterId, "static recurring via [JobMasterClusterId] + schedule attribute");
    }

    private static async Task AssertRanOnAsync(ScenarioRunner runner, string testIdentifier, int expectedCount, string expectedClusterId, string path)
    {
        var executions = await runner.Tracker.WaitForAsync(testIdentifier, expectedCount, ExecuteWaitTimeout);
        executions.Should().OnlyContain(e => e.ClusterId == expectedClusterId, $"{path} should run on '{expectedClusterId}'");
    }

    private static async Task AssertJobsStoredOnAsync(
        IScenarioApiClient api, string testIdentifier, List<Guid> jobIds, string expectedClusterId, string otherClusterId)
    {
        var jobs = await api.GetJobsAsync(expectedClusterId, testIdentifier: testIdentifier, countLimit: int.MaxValue);
        jobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(jobIds,
            $"every job for '{testIdentifier}' should be stored on '{expectedClusterId}'");

        (await api.GetJobCountAsync(otherClusterId, testIdentifier: testIdentifier)).Should().Be(0,
            $"no job for '{testIdentifier}' should be stored on '{otherClusterId}'");
    }
}
