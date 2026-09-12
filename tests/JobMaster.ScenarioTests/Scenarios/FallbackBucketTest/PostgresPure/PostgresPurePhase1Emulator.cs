using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.FallbackBucketTest.PostgresPure;

public sealed class PostgresPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : FallbackBucketTestPhase1EmulatorBase<PostgresPurePhases>(global, runner)
{
    protected override string ClusterId => "fallback-bucket";
    protected override string ContainerName => "pg-fallback";

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase1;
}
