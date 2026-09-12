using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest.RavenDbPure;

public sealed class RavenDbPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : MigratingModeTestPhase1EmulatorBase<RavenDbPurePhases>(global, runner)
{
    protected override string SourceClusterId => "migrating-source";
    protected override string TargetClusterId => "migrating-target";
    protected override string ContainerName => "raven-migration";

    public override RavenDbPurePhases Phase() => RavenDbPurePhases.Phase1;
}
