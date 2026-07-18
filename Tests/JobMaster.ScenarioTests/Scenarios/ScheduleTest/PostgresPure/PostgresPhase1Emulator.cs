using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.PostgresPure;

public sealed class PostgresPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : ScheduleTestPhase1EmulatorBase<PostgresPurePhases>(global, runner)
{
    protected override IReadOnlyList<string> ClusterIds { get; } = Enum.GetValues<PostgresPureClusters>()
        .Select(c => c.ToString()!.ToKebabCase())
        .ToList();

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase1;
}
