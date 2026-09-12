namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.PostgresPure;

public sealed class PostgresPurePhase1Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : ArchivedModeTestPhase1EmulatorBase<PostgresPurePhases>(global, runner)
{
    protected override string SourceClusterId => "archive-source";
    protected override string TargetClusterId => "archive-target";
    protected override string ContainerName => "pg-archive";

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase1;
}
