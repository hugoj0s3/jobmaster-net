using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.MySqlPure;

public sealed class MySqlPurePhase3Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : DataRetentionPhase3EmulatorBase<MySqlPurePhases>(global, runner)
{
    protected override string DrainClusterId => MySqlPureClusters.MysqlDistOne.ToString().ToKebabCase();

    public override MySqlPurePhases Phase() => MySqlPurePhases.Phase3;
}
