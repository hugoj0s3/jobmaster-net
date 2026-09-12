using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.MySqlPure;

public sealed class MySqlPurePhase2Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : DataRetentionPhase2EmulatorBase<MySqlPurePhases>(global, runner)
{
    protected override string TtlOnlyClusterId => MySqlPureClusters.MysqlStandalone.ToString().ToKebabCase();
    protected override string DrainClusterId => MySqlPureClusters.MysqlDistOne.ToString().ToKebabCase();
    protected override string ControlClusterId => MySqlPureClusters.MysqlDistTwo.ToString().ToKebabCase();

    public override MySqlPurePhases Phase() => MySqlPurePhases.Phase2;
}
