using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.MySqlPure;

public sealed class MySqlPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : ScheduleTestPhase1EmulatorBase<MySqlPurePhases>(global, runner)
{
    protected override IReadOnlyList<string> ClusterIds { get; } = Enum.GetValues<MySqlPureClusters>()
        .Select(c => c.ToString()!.ToKebabCase())
        .ToList();

    public override MySqlPurePhases Phase() => MySqlPurePhases.Phase1;
}
