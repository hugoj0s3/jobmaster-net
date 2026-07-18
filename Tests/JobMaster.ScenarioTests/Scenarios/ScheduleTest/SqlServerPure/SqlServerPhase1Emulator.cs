using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.SqlServerPure;

public sealed class SqlServerPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : ScheduleTestPhase1EmulatorBase<SqlServerPurePhases>(global, runner)
{
    protected override IReadOnlyList<string> ClusterIds { get; } = Enum.GetValues<SqlServerPureClusters>()
        .Select(c => c.ToString()!.ToKebabCase())
        .ToList();

    public override SqlServerPurePhases Phase() => SqlServerPurePhases.Phase1;
}
