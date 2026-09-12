using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.RavenDbPure;

public sealed class RavenDbPurePhase3Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : DataRetentionPhase3EmulatorBase<RavenDbPurePhases>(global, runner)
{
    protected override string DrainClusterId => RavenDbPureClusters.RavendbDistOne.ToString().ToKebabCase();

    public override RavenDbPurePhases Phase() => RavenDbPurePhases.Phase3;
}
