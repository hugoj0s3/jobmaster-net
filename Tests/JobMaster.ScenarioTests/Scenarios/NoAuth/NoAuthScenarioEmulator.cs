using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ApiAuths.NoAuth;

public sealed class NoAuthScenarioEmulator(ScenarioGlobalEnvironment global)
    : BaseScenarioEmulator<NoAuthClusters, NoAuthPhases>(global);
