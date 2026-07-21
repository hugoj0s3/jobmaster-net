using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.MigratingModeTest.PostgresNats;

public sealed class PostgresNatsPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : MigratingModeTestPhase1EmulatorBase<PostgresNatsPhases>(global, runner)
{
    protected override string SourceClusterId => "migrating-source";
    protected override string TargetClusterId => "migrating-target";
    protected override string ContainerName => "postgres-nats-migration";

    public override PostgresNatsPhases Phase() => PostgresNatsPhases.Phase1;
}
