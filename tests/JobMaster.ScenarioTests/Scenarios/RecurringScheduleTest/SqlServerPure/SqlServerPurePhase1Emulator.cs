using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.SqlServerPure;

public sealed class SqlServerPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : RecurringScheduleTestPhase1EmulatorBase<SqlServerPurePhases>(global, runner)
{
    protected override string ClusterId => SqlServerPureClusters.SqlserverRecurring.ToString().ToKebabCase();

    public override SqlServerPurePhases Phase() => SqlServerPurePhases.Phase1;
}
