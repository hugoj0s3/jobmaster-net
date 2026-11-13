using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.HybridNatsRavenDb;

public sealed class HybridNatsRavenDbPhase3Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : DataRetentionPhase3EmulatorBase<HybridNatsRavenDbPhases>(global, runner)
{
    protected override string DrainClusterId => HybridNatsRavenDbClusters.HybridDistOne.ToString().ToKebabCase();

    public override HybridNatsRavenDbPhases Phase() => HybridNatsRavenDbPhases.Phase3;
}
