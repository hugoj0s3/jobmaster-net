using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest.HybridNatsRavenDb;

public sealed class HybridNatsRavenDbPhase2Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : DataRetentionPhase2EmulatorBase<HybridNatsRavenDbPhases>(global, runner)
{
    // No TtlOnlyClusterId: TTL purge is master-side only, already covered by the *Pure scenarios.
    // This phase drains buckets whose save-pending and processing sides live on different providers.
    protected override string DrainClusterId => HybridNatsRavenDbClusters.HybridDistOne.ToString().ToKebabCase();
    protected override string ControlClusterId => HybridNatsRavenDbClusters.HybridDistTwo.ToString().ToKebabCase();

    public override HybridNatsRavenDbPhases Phase() => HybridNatsRavenDbPhases.Phase2;
}
