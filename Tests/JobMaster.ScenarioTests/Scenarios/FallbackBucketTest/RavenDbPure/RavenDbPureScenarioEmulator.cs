using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.FallbackBucketTest.RavenDbPure;

public sealed class RavenDbPureScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<RavenDbPureClusters, RavenDbPurePhases>(global);
