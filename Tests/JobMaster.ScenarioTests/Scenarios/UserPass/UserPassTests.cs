using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioTests.Scenarios.ApiAuths.UserPass;

public sealed class UserPassTests(ScenarioGlobalEnvironment global)
    : BaseScenarioTest<UserPassScenarioEmulator, UserPassClusters, UserPassPhases>(global);
