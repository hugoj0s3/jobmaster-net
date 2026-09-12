using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.PostgresNats;

public sealed class PostgresNatsPhase3Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : DataRetentionPhase3EmulatorBase<PostgresNatsPhases>(global, runner)
{
    protected override string DrainClusterId => PostgresNatsClusters.PostgresNatsDistOne.ToString().ToKebabCase();

    public override PostgresNatsPhases Phase() => PostgresNatsPhases.Phase3;
}
