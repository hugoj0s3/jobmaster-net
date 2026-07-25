namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.PostgresNats;

public sealed class PostgresNatsPhase2Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : StandaloneToDistributedTestPhase2EmulatorBase<PostgresNatsPhases>(global, runner)
{
    protected override string ClusterId => "standalone-to-dist";
    protected override string AgentConnectionName => "nats-agent-dist";

    protected override (string TestIdentifier, List<Guid> JobIds) LoadState() =>
        (PostgresNatsState.TestIdentifier, PostgresNatsState.ScheduledJobIds);

    public override PostgresNatsPhases Phase() => PostgresNatsPhases.Phase2;
}
