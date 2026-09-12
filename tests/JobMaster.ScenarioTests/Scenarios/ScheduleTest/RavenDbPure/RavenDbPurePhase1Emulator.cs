using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.RavenDbPure;

public sealed class RavenDbPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : ScheduleTestPhase1EmulatorBase<RavenDbPurePhases>(global, runner)
{
    protected override IReadOnlyList<string> ClusterIds { get; } = Enum.GetValues<RavenDbPureClusters>()
        .Select(c => c.ToString()!.ToKebabCase())
        .ToList();

    public override RavenDbPurePhases Phase() => RavenDbPurePhases.Phase1;
}
