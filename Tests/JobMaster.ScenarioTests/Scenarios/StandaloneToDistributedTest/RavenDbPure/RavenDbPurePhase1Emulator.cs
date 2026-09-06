namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest.RavenDbPure;

public sealed class RavenDbPurePhase1Emulator(JobMaster.ScenarioTests.Fixtures.ScenarioGlobalEnvironment global, JobMaster.ScenarioTests.Runner.ScenarioRunner runner)
    : StandaloneToDistributedTestPhase1EmulatorBase<RavenDbPurePhases>(global, runner)
{
    protected override string ClusterId => "standalone-to-dist";
    protected override string ContainerName => "ravendb-standalone";

    protected override void StoreState(string testIdentifier, List<Guid> jobIds)
    {
        RavenDbPureState.TestIdentifier = testIdentifier;
        RavenDbPureState.ScheduledJobIds = jobIds;
    }

    public override RavenDbPurePhases Phase() => RavenDbPurePhases.Phase1;
}
