using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.RavenDbDist;

public sealed class RavenDbDistTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<RavenDbDistScenarioEmulator, RavenDbDistClusters, RavenDbDistPhases>(global);
