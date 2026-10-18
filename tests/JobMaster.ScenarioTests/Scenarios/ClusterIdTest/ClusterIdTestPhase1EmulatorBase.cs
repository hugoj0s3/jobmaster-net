using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using JobMaster.ScenarioTests.Scenarios.ScheduleTest;

namespace JobMaster.ScenarioTests.Scenarios.ClusterIdTest;

/// <summary>
/// Shared phase-1 logic for every repo-type variant of this scenario: one container
/// (<c>TargetTestClusterIdApp</c>) hosts a default cluster and a target cluster, and every path below is
/// scheduled/registered <b>without</b> an explicit clusterId -- the cluster must come from the handler,
/// definition or profile:
/// <list type="bullet">
/// <item>one-off via <c>[JobMasterClusterId]</c> on the handler → target;</item>
/// <item>one-off via <c>Advanced.OnceNowAsync&lt;TDefinition&gt;()</c>, <c>Config.ClusterId</c> → target;</item>
/// <item>static recurring via <c>[JobMasterClusterId]</c> + <c>[TimeSpanIntervalSchedule]</c>, no profile → target;</item>
/// <item>static recurring via a profile's <c>ClusterId</c> on a plain handler → target;</item>
/// <item>control: plain handler, no cluster id anywhere → default.</item>
/// </list>
/// Each path is checked two ways: the <c>ClusterId</c> the handler actually ran with (recorded to Redis by
/// the app), and where the API finds the job/schedule -- on the expected cluster and not on the other one.
///
/// Uses its own target app (not TargetTestScheduleApp/TargetTestRecurringApp): a handler/definition/profile
/// naming a cluster that isn't configured fails startup validation, so putting these in a shared app would
/// break every other scenario that runs it.
/// </summary>
public abstract class ClusterIdTestPhase1EmulatorBase<TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TPhaseEnum : struct, Enum
{
    private const int QtyJobs = 5;

    // Must match TargetTestClusterIdApp's literals (different process/project).
    private const string ProfileTestIdentifier = "cluster-id-profile";
    private const string AttributeRecurringTestIdentifier = "cluster-id-attribute-recurring";
    private const string ProfileJobDefinitionId = "ClusterIdApp.ProfileTick";
    private const string AttributeRecurringJobDefinitionId = "ClusterIdApp.AttributeRecurring";

    private static readonly TimeSpan ExecuteWaitTimeout = TimeSpan.FromMinutes(3);

    protected abstract string DefaultClusterId { get; }
    protected abstract string TargetClusterId { get; }
    protected abstract string ContainerName { get; }

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");
        var schedule = Runner.ScheduleFor(ContainerName);

        // Static identifiers are fixed literals baked into the app and Redis is shared across the whole run
        // (and across repo-type variants) -- clear so only this run's executions are seen. The static schedules
        // keep firing every 15s, so clearing after the container started loses nothing that matters.
        await Runner.Tracker.ClearAsync(ProfileTestIdentifier);
        await Runner.Tracker.ClearAsync(AttributeRecurringTestIdentifier);

        // Fresh per run -- same reasoning as MigratingModeTestPhase1EmulatorBase.
        var attributeId = Guid.NewGuid().ToString("N");
        var advancedId = Guid.NewGuid().ToString("N");
        var defaultId = Guid.NewGuid().ToString("N");

        var attribute = await schedule.ScheduleAsync("attribute", attributeId, QtyJobs);
        var advanced = await schedule.ScheduleAsync("advanced", advancedId, QtyJobs);
        var control = await schedule.ScheduleAsync("default", defaultId, QtyJobs);

        await AssertRanOnAsync(attributeId, QtyJobs, TargetClusterId, "one-off via [JobMasterClusterId]");
        await AssertRanOnAsync(advancedId, QtyJobs, TargetClusterId, "one-off via Advanced definition Config.ClusterId");
        await AssertRanOnAsync(defaultId, QtyJobs, DefaultClusterId, "one-off with no cluster id (control)");

        await AssertJobsStoredOnAsync(api, attributeId, attribute.JobIds, TargetClusterId, DefaultClusterId);
        await AssertJobsStoredOnAsync(api, advancedId, advanced.JobIds, TargetClusterId, DefaultClusterId);
        await AssertJobsStoredOnAsync(api, defaultId, control.JobIds, DefaultClusterId, TargetClusterId);

        // Static schedules are registered at startup, so they already exist by now.
        var targetSchedules = await api.GetRecurringSchedulesAsync(TargetClusterId);
        var defaultSchedules = await api.GetRecurringSchedulesAsync(DefaultClusterId);

        targetSchedules.Should().ContainSingle(s => s.JobDefinitionId == ProfileJobDefinitionId,
            "the profile's ClusterId should register its schedule on the target cluster");
        targetSchedules.Should().ContainSingle(s => s.JobDefinitionId == AttributeRecurringJobDefinitionId,
            "[JobMasterClusterId] on a [TimeSpanIntervalSchedule] handler should register its schedule on the target cluster");
        defaultSchedules.Should().NotContain(s => s.JobDefinitionId == ProfileJobDefinitionId || s.JobDefinitionId == AttributeRecurringJobDefinitionId,
            "neither static schedule should be registered on the default cluster");

        await AssertRanOnAsync(ProfileTestIdentifier, 1, TargetClusterId, "static recurring via profile ClusterId");
        await AssertRanOnAsync(AttributeRecurringTestIdentifier, 1, TargetClusterId, "static recurring via [JobMasterClusterId] + schedule attribute");
    }

    private async Task AssertRanOnAsync(string testIdentifier, int expectedCount, string expectedClusterId, string path)
    {
        var executions = await Runner.Tracker.WaitForAsync(testIdentifier, expectedCount, ExecuteWaitTimeout);
        executions.Should().OnlyContain(e => e.ClusterId == expectedClusterId,
            $"{path} should run on '{expectedClusterId}'");
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
