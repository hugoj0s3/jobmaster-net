using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.HybridNatsRavenDb;

public sealed class HybridNatsRavenDbPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : ScheduleTestPhase1EmulatorBase<HybridNatsRavenDbPhases>(global, runner)
{
    protected override IReadOnlyList<string> ClusterIds { get; } = Enum.GetValues<HybridNatsRavenDbClusters>()
        .Select(c => c.ToString()!.ToKebabCase())
        .ToList();

    public override HybridNatsRavenDbPhases Phase() => HybridNatsRavenDbPhases.Phase1;
}
