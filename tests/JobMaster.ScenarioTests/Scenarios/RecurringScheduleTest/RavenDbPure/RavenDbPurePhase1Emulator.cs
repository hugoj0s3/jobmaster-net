using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.RavenDbPure;

public sealed class RavenDbPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : RecurringScheduleTestPhase1EmulatorBase<RavenDbPurePhases>(global, runner)
{
    protected override string ClusterId => RavenDbPureClusters.RavendbRecurring.ToString().ToKebabCase();

    public override RavenDbPurePhases Phase() => RavenDbPurePhases.Phase1;
}
