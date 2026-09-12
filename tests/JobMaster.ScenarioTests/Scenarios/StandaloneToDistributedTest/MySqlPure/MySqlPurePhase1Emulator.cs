namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.MySqlPure;

public sealed class MySqlPurePhase1Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : StandaloneToDistributedTestPhase1EmulatorBase<MySqlPurePhases>(global, runner)
{
    protected override string ClusterId => "standalone-to-dist";
    protected override string ContainerName => "mysql-standalone";

    protected override void StoreState(string testIdentifier, List<Guid> jobIds)
    {
        MySqlPureState.TestIdentifier = testIdentifier;
        MySqlPureState.ScheduledJobIds = jobIds;
    }

    public override MySqlPurePhases Phase() => MySqlPurePhases.Phase1;
}
