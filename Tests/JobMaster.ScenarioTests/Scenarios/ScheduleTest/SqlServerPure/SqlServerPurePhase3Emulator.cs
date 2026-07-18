using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.SqlServerPure;

public sealed class SqlServerPurePhase3Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : DataRetentionPhase3EmulatorBase<SqlServerPurePhases>(global, runner)
{
    protected override string DrainClusterId => SqlServerPureClusters.SqlserverDistOne.ToString().ToKebabCase();

    public override SqlServerPurePhases Phase() => SqlServerPurePhases.Phase3;
}
