using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.SqlServerPure;

public sealed class SqlServerPurePhase2Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : DataRetentionPhase2EmulatorBase<SqlServerPurePhases>(global, runner)
{
    protected override string TtlOnlyClusterId => SqlServerPureClusters.SqlserverStandalone.ToString().ToKebabCase();
    protected override string DrainClusterId => SqlServerPureClusters.SqlserverDistOne.ToString().ToKebabCase();
    protected override string ControlClusterId => SqlServerPureClusters.SqlserverDistTwo.ToString().ToKebabCase();

    public override SqlServerPurePhases Phase() => SqlServerPurePhases.Phase2;
}
