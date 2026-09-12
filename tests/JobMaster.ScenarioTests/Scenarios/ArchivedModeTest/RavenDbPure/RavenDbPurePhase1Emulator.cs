namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.RavenDbPure;

public sealed class RavenDbPurePhase1Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : ArchivedModeTestPhase1EmulatorBase<RavenDbPurePhases>(global, runner)
{
    protected override string SourceClusterId => "archive-source";
    protected override string TargetClusterId => "archive-target";
    protected override string ContainerName => "raven-archive";

    public override RavenDbPurePhases Phase() => RavenDbPurePhases.Phase1;
}
