using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.RavenDbPure;

public sealed class RavenDbPurePhase2Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : DataRetentionPhase2EmulatorBase<RavenDbPurePhases>(global, runner)
{
    protected override string TtlOnlyClusterId => RavenDbPureClusters.RavendbStandalone.ToString().ToKebabCase();
    protected override string DrainClusterId => RavenDbPureClusters.RavendbDistOne.ToString().ToKebabCase();
    protected override string ControlClusterId => RavenDbPureClusters.RavendbDistTwo.ToString().ToKebabCase();

    public override RavenDbPurePhases Phase() => RavenDbPurePhases.Phase2;
}
