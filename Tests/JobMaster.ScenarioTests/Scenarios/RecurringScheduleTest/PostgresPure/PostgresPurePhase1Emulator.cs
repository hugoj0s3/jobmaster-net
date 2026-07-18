using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.PostgresPure;

public sealed class PostgresPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : RecurringScheduleTestPhase1EmulatorBase<PostgresPurePhases>(global, runner)
{
    protected override string ClusterId => PostgresPureClusters.PostgresRecurring.ToString().ToKebabCase();

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase1;
}
