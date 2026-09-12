using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.PostgresPure;

public sealed class PostgresPurePhase3Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : DataRetentionPhase3EmulatorBase<PostgresPurePhases>(global, runner)
{
    protected override string DrainClusterId => PostgresPureClusters.PostgresDistOne.ToString().ToKebabCase();

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase3;
}
