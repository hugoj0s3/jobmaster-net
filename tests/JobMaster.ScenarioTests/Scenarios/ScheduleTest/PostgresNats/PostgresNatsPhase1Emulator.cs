using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.PostgresNats;

public sealed class PostgresNatsPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : ScheduleTestPhase1EmulatorBase<PostgresNatsPhases>(global, runner)
{
    protected override IReadOnlyList<string> ClusterIds { get; } = Enum.GetValues<PostgresNatsClusters>()
        .Select(c => c.ToString()!.ToKebabCase())
        .ToList();

    public override PostgresNatsPhases Phase() => PostgresNatsPhases.Phase1;
}
