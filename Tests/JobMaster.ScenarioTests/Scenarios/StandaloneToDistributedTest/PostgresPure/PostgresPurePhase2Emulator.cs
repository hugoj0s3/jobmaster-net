namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.PostgresPure;

public sealed class PostgresPurePhase2Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : StandaloneToDistributedTestPhase2EmulatorBase<PostgresPurePhases>(global, runner)
{
    protected override string ClusterId => "standalone-to-dist";
    protected override string AgentConnectionName => "pg-agent-dist";

    protected override (string TestIdentifier, List<Guid> JobIds) LoadState() =>
        (PostgresPureState.TestIdentifier, PostgresPureState.ScheduledJobIds);

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase2;
}
