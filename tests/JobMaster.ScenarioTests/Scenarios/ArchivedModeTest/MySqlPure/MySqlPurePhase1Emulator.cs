namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.MySqlPure;

public sealed class MySqlPurePhase1Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : ArchivedModeTestPhase1EmulatorBase<MySqlPurePhases>(global, runner)
{
    protected override string SourceClusterId => "archive-source";
    protected override string TargetClusterId => "archive-target";
    protected override string ContainerName => "mysql-archive";

    public override MySqlPurePhases Phase() => MySqlPurePhases.Phase1;
}
