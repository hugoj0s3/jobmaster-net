namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.SqlServerPure;

public sealed class SqlServerPurePhase1Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : ArchivedModeTestPhase1EmulatorBase<SqlServerPurePhases>(global, runner)
{
    protected override string SourceClusterId => "archive-source";
    protected override string TargetClusterId => "archive-target";
    protected override string ContainerName => "sqlserver-archive";

    public override SqlServerPurePhases Phase() => SqlServerPurePhases.Phase1;
}
