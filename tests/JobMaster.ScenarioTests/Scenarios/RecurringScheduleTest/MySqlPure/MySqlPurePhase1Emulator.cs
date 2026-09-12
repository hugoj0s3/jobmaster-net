using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest.MySqlPure;

public sealed class MySqlPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : RecurringScheduleTestPhase1EmulatorBase<MySqlPurePhases>(global, runner)
{
    protected override string ClusterId => MySqlPureClusters.MysqlRecurring.ToString().ToKebabCase();

    public override MySqlPurePhases Phase() => MySqlPurePhases.Phase1;
}
