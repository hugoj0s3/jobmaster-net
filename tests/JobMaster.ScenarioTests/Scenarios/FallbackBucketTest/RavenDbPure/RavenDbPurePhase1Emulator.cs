using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.FallbackBucketTest.RavenDbPure;

public sealed class RavenDbPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : FallbackBucketTestPhase1EmulatorBase<RavenDbPurePhases>(global, runner)
{
    protected override string ClusterId => "fallback-bucket";
    protected override string ContainerName => "raven-fallback";

    public override RavenDbPurePhases Phase() => RavenDbPurePhases.Phase1;
}
