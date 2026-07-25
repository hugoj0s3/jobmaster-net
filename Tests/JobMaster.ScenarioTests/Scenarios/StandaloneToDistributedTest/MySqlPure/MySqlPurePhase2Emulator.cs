namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.MySqlPure;

public sealed class MySqlPurePhase2Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : StandaloneToDistributedTestPhase2EmulatorBase<MySqlPurePhases>(global, runner)
{
    protected override string ClusterId => "standalone-to-dist";
    protected override string AgentConnectionName => "mysql-agent-dist";

    protected override (string TestIdentifier, List<Guid> JobIds) LoadState() =>
        (MySqlPureState.TestIdentifier, MySqlPureState.ScheduledJobIds);

    public override MySqlPurePhases Phase() => MySqlPurePhases.Phase2;
}
