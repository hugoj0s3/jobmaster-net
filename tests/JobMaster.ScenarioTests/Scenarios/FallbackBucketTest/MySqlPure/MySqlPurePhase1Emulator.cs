using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.FallbackBucketTest.MySqlPure;

public sealed class MySqlPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : FallbackBucketTestPhase1EmulatorBase<MySqlPurePhases>(global, runner)
{
    protected override string ClusterId => "fallback-bucket";
    protected override string ContainerName => "mysql-fallback";

    public override MySqlPurePhases Phase() => MySqlPurePhases.Phase1;
}
