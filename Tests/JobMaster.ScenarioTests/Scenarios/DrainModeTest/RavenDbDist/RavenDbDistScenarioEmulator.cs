using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.DrainModeTest.RavenDbDist;

public sealed class RavenDbDistScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<RavenDbDistClusters, RavenDbDistPhases>(global);
