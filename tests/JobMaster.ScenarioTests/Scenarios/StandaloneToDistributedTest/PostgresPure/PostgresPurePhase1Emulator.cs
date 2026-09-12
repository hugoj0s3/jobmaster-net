namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.PostgresPure;

public sealed class PostgresPurePhase1Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : StandaloneToDistributedTestPhase1EmulatorBase<PostgresPurePhases>(global, runner)
{
    protected override string ClusterId => "standalone-to-dist";
    protected override string ContainerName => "pg-standalone";

    protected override void StoreState(string testIdentifier, List<Guid> jobIds)
    {
        PostgresPureState.TestIdentifier = testIdentifier;
        PostgresPureState.ScheduledJobIds = jobIds;
    }

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase1;
}
