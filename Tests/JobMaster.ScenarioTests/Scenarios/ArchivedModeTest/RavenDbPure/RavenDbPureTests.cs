using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest.RavenDbPure;

public sealed class RavenDbPureTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<RavenDbPureScenarioEmulator, RavenDbPureClusters, RavenDbPurePhases>(global);
