using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.BasicExecution;

public sealed class BasicExecutionPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<BasicExecutionPhases>(global, runner)
{
    public override BasicExecutionPhases Phase() => BasicExecutionPhases.Phase1;

    public override async Task RunAsync()
    {
        var testIdentifier = Guid.NewGuid().ToString("N");

        var scheduled = await Runner.Schedule.ScheduleAsync("fast", testIdentifier, qtyJobs: 1);
        scheduled.JobIds.Should().HaveCount(1);

        // This scenario shares its ClusterId + database with JwtAuthTests. When this phase runs
        // after another test already used this cluster in the same process, this container's
        // worker is a fresh host taking over from one that just died — JobMaster's liveness-based
        // bucket recovery needs noticeably longer than a clean first-run pickup (which takes ~2s).
        var executions = await Runner.Tracker.WaitForAsync(testIdentifier, expectedCount: 1, timeout: TimeSpan.FromSeconds(90));

        executions.Should().HaveCount(1);
        executions.Select(e => e.JobId).Should().Equal(scheduled.JobIds);

        // Re-read after a settle window to prove no duplicate delivery, not just "at least 1 yet".
        await Task.Delay(TimeSpan.FromSeconds(2));
        var all = await Runner.Tracker.GetAllAsync(testIdentifier);
        all.Should().HaveCount(1);
        all.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        Runner.Api.Should().NotBeNull();
        var clusterId = BasicExecutionClusters.BasicExecutionCluster.ToString().ToKebabCase();
        var apiExecutions = await Runner.Api!.GetJobExecutionsAsync(clusterId, scheduled.JobIds[0]);
        apiExecutions.Should().NotBeEmpty();
    }
}
