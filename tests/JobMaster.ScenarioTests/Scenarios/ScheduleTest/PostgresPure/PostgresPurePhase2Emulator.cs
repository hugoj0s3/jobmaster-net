using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.PostgresPure;

public sealed class PostgresPurePhase2Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : DataRetentionPhase2EmulatorBase<PostgresPurePhases>(global, runner)
{
    protected override string TtlOnlyClusterId => PostgresPureClusters.PostgresStandalone.ToString().ToKebabCase();
    protected override string DrainClusterId => PostgresPureClusters.PostgresDistOne.ToString().ToKebabCase();
    protected override string ControlClusterId => PostgresPureClusters.PostgresDistTwo.ToString().ToKebabCase();

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase2;
}
