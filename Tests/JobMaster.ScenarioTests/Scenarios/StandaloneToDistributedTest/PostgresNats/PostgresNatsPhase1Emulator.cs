namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.PostgresNats;

public sealed class PostgresNatsPhase1Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : StandaloneToDistributedTestPhase1EmulatorBase<PostgresNatsPhases>(global, runner)
{
    protected override string ClusterId => "standalone-to-dist";
    protected override string ContainerName => "postgres-nats-standalone";

    protected override void StoreState(string testIdentifier, List<Guid> jobIds)
    {
        PostgresNatsState.TestIdentifier = testIdentifier;
        PostgresNatsState.ScheduledJobIds = jobIds;
    }

    public override PostgresNatsPhases Phase() => PostgresNatsPhases.Phase1;
}
