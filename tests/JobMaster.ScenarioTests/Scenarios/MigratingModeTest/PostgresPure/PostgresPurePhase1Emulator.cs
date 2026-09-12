using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest.PostgresPure;

public sealed class PostgresPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : MigratingModeTestPhase1EmulatorBase<PostgresPurePhases>(global, runner)
{
    protected override string SourceClusterId => "migrating-source";
    protected override string TargetClusterId => "migrating-target";
    protected override string ContainerName => "pg-migration";

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase1;
}
