namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.RavenDbPure;

public sealed class RavenDbPurePhase2Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : StandaloneToDistributedTestPhase2EmulatorBase<RavenDbPurePhases>(global, runner)
{
    protected override string ClusterId => "standalone-to-dist";
    protected override string AgentConnectionName => "ravendb-agent-dist";

    protected override (string TestIdentifier, List<Guid> JobIds) LoadState() =>
        (RavenDbPureState.TestIdentifier, RavenDbPureState.ScheduledJobIds);

    public override RavenDbPurePhases Phase() => RavenDbPurePhases.Phase2;
}
