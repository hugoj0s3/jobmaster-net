namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.PostgresNats;

public sealed class PostgresNatsPhase1Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : ArchivedModeTestPhase1EmulatorBase<PostgresNatsPhases>(global, runner)
{
    protected override string SourceClusterId => "archive-source";
    protected override string TargetClusterId => "archive-target";
    protected override string ContainerName => "postgres-nats-archive";

    public override PostgresNatsPhases Phase() => PostgresNatsPhases.Phase1;
}
