using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ApiAuths.NoAuth;

public sealed class NoAuthTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<NoAuthScenarioEmulator, NoAuthClusters, NoAuthPhases>(global);
