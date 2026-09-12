using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest.SqlServerPure;

public sealed class SqlServerPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : MigratingModeTestPhase1EmulatorBase<SqlServerPurePhases>(global, runner)
{
    protected override string SourceClusterId => "migrating-source";
    protected override string TargetClusterId => "migrating-target";
    protected override string ContainerName => "sqlserver-migration";

    public override SqlServerPurePhases Phase() => SqlServerPurePhases.Phase1;
}
