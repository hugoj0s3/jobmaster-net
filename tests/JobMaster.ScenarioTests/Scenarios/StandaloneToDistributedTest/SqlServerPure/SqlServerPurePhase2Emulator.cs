namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.SqlServerPure;

public sealed class SqlServerPurePhase2Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : StandaloneToDistributedTestPhase2EmulatorBase<SqlServerPurePhases>(global, runner)
{
    protected override string ClusterId => "standalone-to-dist";
    protected override string AgentConnectionName => "sqlserver-agent-dist";

    protected override (string TestIdentifier, List<Guid> JobIds) LoadState() =>
        (SqlServerPureState.TestIdentifier, SqlServerPureState.ScheduledJobIds);

    public override SqlServerPurePhases Phase() => SqlServerPurePhases.Phase2;
}
