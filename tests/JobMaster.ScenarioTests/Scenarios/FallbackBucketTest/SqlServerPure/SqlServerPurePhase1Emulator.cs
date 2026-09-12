using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.FallbackBucketTest.SqlServerPure;

public sealed class SqlServerPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : FallbackBucketTestPhase1EmulatorBase<SqlServerPurePhases>(global, runner)
{
    protected override string ClusterId => "fallback-bucket";
    protected override string ContainerName => "sqlserver-fallback";

    public override SqlServerPurePhases Phase() => SqlServerPurePhases.Phase1;
}
