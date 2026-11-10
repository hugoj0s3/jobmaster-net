using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.HybridNatsRavenDb;

public sealed class HybridNatsRavenDbPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : RecurringScheduleTestPhase1EmulatorBase<HybridNatsRavenDbPhases>(global, runner)
{
    protected override string ClusterId => HybridNatsRavenDbClusters.HybridRecurring.ToString().ToKebabCase();

    public override HybridNatsRavenDbPhases Phase() => HybridNatsRavenDbPhases.Phase1;
}
