namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.SqlServerPure;

public sealed class SqlServerPurePhase1Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : StandaloneToDistributedTestPhase1EmulatorBase<SqlServerPurePhases>(global, runner)
{
    protected override string ClusterId => "standalone-to-dist";
    protected override string ContainerName => "sqlserver-standalone";

    protected override void StoreState(string testIdentifier, List<Guid> jobIds)
    {
        SqlServerPureState.TestIdentifier = testIdentifier;
        SqlServerPureState.ScheduledJobIds = jobIds;
    }

    public override SqlServerPurePhases Phase() => SqlServerPurePhases.Phase1;
}
