using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.FallbackBucketTest.RavenDbPure;

public sealed class RavenDbPureTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<RavenDbPureScenarioEmulator, RavenDbPureClusters, RavenDbPurePhases>(global);
