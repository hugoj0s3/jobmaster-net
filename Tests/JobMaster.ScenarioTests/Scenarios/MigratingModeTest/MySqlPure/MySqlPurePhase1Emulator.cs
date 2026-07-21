using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest.MySqlPure;

public sealed class MySqlPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : MigratingModeTestPhase1EmulatorBase<MySqlPurePhases>(global, runner)
{
    protected override string SourceClusterId => "migrating-source";
    protected override string TargetClusterId => "migrating-target";
    protected override string ContainerName => "mysql-migration";

    public override MySqlPurePhases Phase() => MySqlPurePhases.Phase1;
}
